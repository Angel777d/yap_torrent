import asyncio
import base64
import logging
import shutil
import time
from pathlib import Path
from typing import Any, Awaitable, Callable, Dict, Tuple, Optional, Set

import aiohttp
from angelovich.core.DataStorage import Entity

from yap_torrent.components.torrent_ec import (
	TorrentEC,
	TorrentStatsEC,
)
from yap_torrent.env import Env
from yap_torrent.protocol import decode
from yap_torrent.protocol.magnet import MagnetInfo
from yap_torrent.protocol.structures import Metainfo
from yap_torrent.components.file_ec import FilePriority
from yap_torrent.systems import get_torrent_entity, get_torrent_name, is_torrent_active
from yap_torrent.systems.stats_system import session_rates
from .mapping import DEFAULT_FIELDS, STALLED_AFTER_SECONDS, build_torrent

logger = logging.getLogger(__name__)

# our section in config.json, for the settings core has no notion of
PLUGIN_CONFIG_KEY = "yap_torrent_transmission_rpc"

_TorrentID = int | str
_TorrentIDs = _TorrentID | list[_TorrentID] | None


class TorrentIDs:
	def __init__(self, source: _TorrentIDs = None):
		self.indexes: set[int] = set()
		self.hashes: set[str] = set()
		self.add(source)

	def add(self, ids: _TorrentIDs):
		if ids is None:
			return
		# bool is a subclass of int; reject it before the int branch so a JSON
		# true/false is not silently treated as index 1/0.
		if isinstance(ids, bool):
			logger.warning("Ignoring boolean torrent id: %s", ids)
		elif isinstance(ids, int):
			self.indexes.add(ids)
		elif isinstance(ids, str):
			self.hashes.add(ids.lower())
		elif isinstance(ids, list):
			for _id in ids:
				self.add(_id)
		else:
			logger.warning("Invalid ids argument: %s", ids)

	def contains(self, torrent: TorrentEC):
		return torrent.index in self.indexes or torrent.info_hash.hex() in self.hashes

	def empty(self):
		return not (self.indexes or self.hashes)

	@staticmethod
	def is_recent(ids):
		return ids in ("recently-active", "recently_active")

	@staticmethod
	def read_ids(arguments, recent: "TorrentIDs") -> "TorrentIDs":
		ids = arguments.get("ids")
		if TorrentIDs.is_recent(ids):
			return recent

		return TorrentIDs(ids)

	@property
	def ids(self) -> _TorrentIDs:
		return list(self.indexes) + list(self.hashes)

	def remove(self, other: "TorrentIDs"):
		self.indexes.difference_update(other.indexes)
		self.hashes.difference_update(other.hashes)

	def clear(self):
		self.indexes.clear()
		self.hashes.clear()


class AltSpeed:
	"""Transmission's "turtle mode": a second set of speed limits and a switch.

	Kept here rather than in core, which has exactly one pair of speed limits — the ones
	in force. Holding a spare pair and choosing between them is a client-side idea, and
	core has no notion of it.

	TODO: once core enforces speed limits, enabling this should push the alt pair into
	 config.speed_limit_* and restore the normal pair on the way out. Nothing enforces
	 anything yet, so for now the values are stored and reported only — swapping them
	 would change nothing except what session-get says.
	"""
	FIELDS = ("alt_speed_down", "alt_speed_up", "alt_speed_enabled")

	def __init__(self, stored: Optional[Dict[str, Any]] = None):
		stored = stored or {}
		self.alt_speed_down: int = int(stored.get("alt_speed_down", 0))
		self.alt_speed_up: int = int(stored.get("alt_speed_up", 0))
		self.alt_speed_enabled: bool = bool(stored.get("alt_speed_enabled", False))

	def export(self) -> Dict[str, Any]:
		return {name: getattr(self, name) for name in self.FIELDS}

	def update(self, values: Dict[str, Any]) -> bool:
		"""Apply the fields present in `values`; returns whether anything moved."""
		changed = False
		for name in self.FIELDS:
			if name not in values:
				continue
			cast = type(getattr(self, name))
			try:
				new_value = cast(values[name])
			except (TypeError, ValueError):
				logger.warning("Ignoring bad value for %s: %r", name, values[name])
				continue
			if getattr(self, name) != new_value:
				setattr(self, name, new_value)
				changed = True
		return changed


class ServerInfo:
	def __init__(self, session_id, start_time, alt_speed: Optional[AltSpeed] = None):
		self.session_id: str = session_id
		self.start_time: float = start_time
		self.recent: TorrentIDs = TorrentIDs()
		self.removed: TorrentIDs = TorrentIDs()
		self.alt_speed: AltSpeed = alt_speed or AltSpeed()


# --- protocol version ------------------------------------------------------
# The legacy (kebab-case `method`/`arguments`/`tag`) protocol, which is what every
# existing remote and the transmission-rpc Python client still speak. Transmission 4.1
# deprecates it in favour of JSON-RPC 2.0 with snake_case names (rpc_version 19); that is
# a separate surface, not a newer version of this one.
#
# MAX is the newest legacy rpc-version whose method set we implement, MIN the oldest a
# client may assume. The semver MUST match MAX in the spec's version table — 17 is 5.3.0
# — because clients gate features on the semver rather than the integer.
RPC_VERSION_MIN_SUPPORTED = 14
RPC_VERSION_MAX_SUPPORTED = 17
RPC_VERSION_SEMVER = "5.3.0"

Handler = Callable[[Env, ServerInfo, Dict[str, Any]], Awaitable[Tuple[str, Dict[str, Any]]]]

METHODS: Dict[str, Handler] = {}


def method(name: str) -> Callable[[Handler], Handler]:
	def register(func: Handler) -> Handler:
		METHODS[name] = func
		return func

	return register


async def fetch_url(url: str) -> Optional[bytes]:
	"""Download a remote .torrent file for torrent-add's filename=URL form."""
	try:
		async with aiohttp.ClientSession() as session:
			async with session.get(url) as resp:
				if resp.status != 200:
					logger.warning("fetch %s returned status %s", url, resp.status)
					return None
				return await resp.read()
	except Exception as ex:
		logger.exception("failed to fetch torrent url %s. %s", url, ex)
		return None


def iterate_torrents(env: Env, ids: TorrentIDs, removed: TorrentIDs):
	for e in env.data_storage.get_collection(TorrentEC):
		torrent = e.get_component(TorrentEC)
		if removed.contains(torrent):
			continue
		if not ids.empty() and not ids.contains(torrent):
			continue
		yield e


# ---------------------------------------------------------------------------
# torrent action requests (rpc-spec 3.1)
# ---------------------------------------------------------------------------
@method("torrent-start")
async def torrent_start(env, info, arguments):
	ids = TorrentIDs.read_ids(arguments, info.recent)
	for entity in iterate_torrents(env, ids, info.removed):
		torrent = entity.get_component(TorrentEC)
		info.recent.add(torrent.index)
		await env.event_bus.dispatch_async("request.torrent.start", torrent.info_hash)
	return "success", {}


# torrent-start-now behaves the same here; yap has no download queue to bypass.
METHODS["torrent-start-now"] = torrent_start


@method("torrent-stop")
async def torrent_stop(env, info, arguments):
	ids = TorrentIDs.read_ids(arguments, info.recent)
	for entity in iterate_torrents(env, ids, info.removed):
		torrent = entity.get_component(TorrentEC)
		info.recent.add(torrent.index)
		await env.event_bus.dispatch_async("request.torrent.stop", torrent.info_hash)
	return "success", {}


@method("torrent-verify")
async def torrent_verify(env, info, arguments):
	ids = TorrentIDs.read_ids(arguments, info.recent)
	for entity in iterate_torrents(env, ids, info.removed):
		torrent = entity.get_component(TorrentEC)
		info.recent.add(torrent.index)
		await env.event_bus.dispatch_async("request.torrent.invalidate", torrent.info_hash)
	return "success", {}


@method("torrent-reannounce")
async def torrent_reannounce(env, info, arguments):
	ids = TorrentIDs.read_ids(arguments, info.recent)
	for entity in iterate_torrents(env, ids, info.removed):
		torrent = entity.get_component(TorrentEC)
		info.recent.add(torrent.index)
		await env.event_bus.dispatch_async("request.torrent.reannounce", torrent.info_hash)
	return "success", {}


# ---------------------------------------------------------------------------
# per-torrent settings (rpc-spec 3.2)
# ---------------------------------------------------------------------------
# Transmission is lenient about arguments it cannot act on, and failing the whole call
# over one unsupported key would break clients that always send a full settings block.
# Still inert: "location" (no move-on-disk in core) and the deprecated trackerAdd /
# trackerRemove / trackerReplace. The bandwidth and ratio keys below ARE accepted and
# stored, but nothing enforces them — see TorrentLimitsEC.
_FILE_PRIORITIES = {
	"priority-high": FilePriority.High,
	"priority-normal": FilePriority.Normal,
	"priority-low": FilePriority.Low,
}

# torrent-set argument -> TorrentLimitsEC field
_LIMIT_ARGUMENTS = {
	"downloadLimit": "download_limit",
	"downloadLimited": "download_limited",
	"uploadLimit": "upload_limit",
	"uploadLimited": "upload_limited",
	"honorsSessionLimits": "honors_session_limits",
	"seedRatioLimit": "seed_ratio_limit",
	"seedRatioMode": "seed_ratio_mode",
	"peer-limit": "peer_limit",
	"bandwidthPriority": "bandwidth_priority",
}


async def _apply_torrent_settings(env, info_hash: bytes, arguments: Dict[str, Any]):
	"""Apply every torrent-set-shaped argument. Shared with torrent-add."""
	labels = arguments.get("labels")
	if labels is not None:
		await env.event_bus.dispatch_async("request.torrent.set_labels", info_hash, labels)

	# an empty list means "all files" to Transmission, which is also what
	# request.file.select reads None as
	wanted = arguments.get("files-wanted")
	if wanted is not None:
		await env.event_bus.dispatch_async("request.file.select", info_hash, wanted or None, True, None)
	unwanted = arguments.get("files-unwanted")
	if unwanted is not None:
		await env.event_bus.dispatch_async("request.file.select", info_hash, unwanted or None, False, None)

	for argument, priority in _FILE_PRIORITIES.items():
		indices = arguments.get(argument)
		if indices is not None:
			await env.event_bus.dispatch_async(
				"request.file.select", info_hash, indices or None, None, priority)

	limits = {field: arguments[key] for key, field in _LIMIT_ARGUMENTS.items() if key in arguments}
	if limits:
		await env.event_bus.dispatch_async("request.torrent.set_limits", info_hash, limits)

	position = arguments.get("queuePosition")
	if position is not None:
		await env.event_bus.dispatch_async("request.torrent.queue_move", info_hash, position)


@method("torrent-set")
async def torrent_set(env, info, arguments):
	ids = TorrentIDs.read_ids(arguments, info.recent)
	for entity in iterate_torrents(env, ids, info.removed):
		torrent = entity.get_component(TorrentEC)
		info.recent.add(torrent.index)
		await _apply_torrent_settings(env, torrent.info_hash, arguments)
	return "success", {}


# ---------------------------------------------------------------------------
# queue movement (rpc-spec 4.7)
# ---------------------------------------------------------------------------
def _queue_mover(direction: str) -> Handler:
	async def handler(env, info, arguments):
		ids = TorrentIDs.read_ids(arguments, info.recent)
		# bottom-ward moves are applied in reverse so a multi-torrent selection keeps its
		# relative order instead of being turned inside out
		entities = list(iterate_torrents(env, ids, info.removed))
		if direction in ("bottom", "down"):
			entities.reverse()
		for entity in entities:
			torrent = entity.get_component(TorrentEC)
			info.recent.add(torrent.index)
			await env.event_bus.dispatch_async("request.torrent.queue_move", torrent.info_hash, direction)
		return "success", {}

	return handler


for _direction in ("top", "up", "down", "bottom"):
	METHODS[f"queue-move-{_direction}"] = _queue_mover(_direction)


# ---------------------------------------------------------------------------
# removing torrents (rpc-spec 3.5)
# ---------------------------------------------------------------------------
@method("torrent-remove")
async def torrent_remove(env: Env, info: ServerInfo, arguments):
	delete_data = bool(arguments.get("delete-local-data", False))
	ids = TorrentIDs.read_ids(arguments, info.recent)
	for entity in iterate_torrents(env, ids, info.removed):
		torrent = entity.get_component(TorrentEC)
		logger.info("[torrent-remove] remove torrent: %s (delete_data=%s)", get_torrent_name(entity), delete_data)

		info.removed.add(torrent.index)
		env.event_bus.dispatch("request.torrent.remove", torrent.info_hash, delete_data)

	return "success", {}


# ---------------------------------------------------------------------------
# torrent accessors (rpc-spec 3.3)
# ---------------------------------------------------------------------------
@method("torrent-get")
async def torrent_get(env, info, arguments):
	fields = arguments.get("fields") or list(DEFAULT_FIELDS)
	table = arguments.get("format") == "table"

	ids = TorrentIDs.read_ids(arguments, info.recent)
	torrents = [build_torrent(e, fields, env) for e in iterate_torrents(env, ids, info.removed)]

	if table:
		# "an array of arrays. The first row holds the keys and each remaining row holds
		# a torrent's values for those keys" — so every row must line up with row 0, and
		# a field the torrent lacks becomes a null rather than a shorter row
		rows: list = [list(fields)]
		rows.extend([torrent.get(field) for field in fields] for torrent in torrents)
		result: Dict[str, Any] = {"torrents": rows}
	else:
		result = {"torrents": torrents}

	if TorrentIDs.is_recent(arguments.get("ids")):
		result["removed"] = info.removed.ids

	# full list requested. Clean up recent caches
	if ids.empty():
		info.recent.remove(info.removed)
		info.removed.clear()

	return "success", result


# ---------------------------------------------------------------------------
# adding a torrent (rpc-spec 3.4)
# ---------------------------------------------------------------------------
@method("torrent-add")
async def torrent_add(env, info, arguments):
	download_dir = arguments.get("download-dir")
	if download_dir:
		download_dir = Path(str(download_dir))
	paused = bool(arguments.get("paused", False))
	metainfo: str = arguments.get("metainfo", "")
	filename: str = arguments.get("filename", "")

	if metainfo:
		try:
			data = base64.b64decode(metainfo)
		except (ValueError, TypeError) as ex:
			return f"invalid metainfo: {ex}", {}
		return await _add_metainfo(env, info, data, download_dir, paused, arguments)

	if filename:
		if filename.startswith("magnet:"):
			return await _add_magnet(env, info, filename, paused)
		if filename.startswith("http://") or filename.startswith("https://"):
			data = await fetch_url(filename)
			if data is None:
				return "download of torrent file failed", {}
			return await _add_metainfo(env, info, data, download_dir, paused, arguments)
		# Otherwise treat it as a local .torrent file path.
		try:
			with open(filename, "rb") as handle:
				data = handle.read()
		except OSError as ex:
			return f"unable to read torrent file: {ex}", {}
		return await _add_metainfo(env, info, data, download_dir, paused, arguments)

	return 'either "filename" or "metainfo" must be included', {}


def _added_stub(entity: Entity) -> Dict[str, Any]:
	return {
		"id": entity.get_component(TorrentEC).index,
		"hashString": entity.get_component(TorrentEC).info_hash.hex(),
		"name": get_torrent_name(entity),
	}


async def _add_metainfo(env: Env, info: ServerInfo, data: bytes, download_dir: Optional[Path], paused: bool,
                        arguments: Dict[str, Any]):
	try:
		file_info = Metainfo(decode(data))
		info_hash = file_info.make_info_hash()
	except Exception as ex:  # noqa: BLE001 - any parse failure is a client error
		return f"invalid or corrupt torrent metainfo: {ex}", {}

	existing = get_torrent_entity(env, info_hash)
	if existing is not None:
		return "success", {"torrent-duplicate": _added_stub(existing)}

	await env.event_bus.dispatch_async("request.metainfo.add", file_info, download_dir)

	torrent_entity = get_torrent_entity(env, info_hash)
	if not torrent_entity:
		return "Add torrent operation failed", {}

	if paused:
		await env.event_bus.dispatch_async("request.torrent.stop", info_hash)

	# torrent-add takes the same labels / files-wanted / priority-* / limit arguments as
	# torrent-set. The file entities are built by a collection listener dispatched as a
	# task, so yield once first or a file selection would land before there are files.
	await asyncio.sleep(0)
	await _apply_torrent_settings(env, info_hash, arguments)

	info.recent.add(torrent_entity.get_component(TorrentEC).index)

	logger.info("torrent-add: added %s", info_hash.hex())
	return "success", {"torrent-added": _added_stub(torrent_entity)}


async def _add_magnet(env: Env, info: ServerInfo, magnet_link: str, paused):
	magnet = MagnetInfo(magnet_link)
	if not magnet.is_valid():
		return "invalid magnet link", {}

	info_hash = magnet.info_hash
	existing = get_torrent_entity(env, info_hash)
	if existing is not None:
		return "success", {"torrent-duplicate": _added_stub(existing)}

	await env.event_bus.dispatch_async("request.magnet.add", magnet_link)

	torrent_entity = get_torrent_entity(env, info_hash)
	if not torrent_entity:
		return "add magnet operation failed", {}

	if paused:
		env.event_bus.dispatch("request.torrent.stop", info_hash)

	info.recent.add(torrent_entity.get_component(TorrentEC).index)

	# a magnet has no metadata yet, so prefer its display name (dn) over the
	# entity's placeholder name until real metadata arrives
	stub = _added_stub(torrent_entity)
	if magnet.name:
		stub["name"] = magnet.name

	logger.info("torrent-add: queued magnet %s, paused %s", info_hash.hex(), paused)
	return "success", {"torrent-added": stub}


# ---------------------------------------------------------------------------
# session accessors (rpc-spec 4.2 / 4.3)
# ---------------------------------------------------------------------------
@method("session-get")
async def session_get(env, info, arguments):
	cfg = env.config
	download_dir = Path(cfg.download_folder).as_posix()
	session = {
		"rpc-version": RPC_VERSION_MAX_SUPPORTED,
		"rpc-version-minimum": RPC_VERSION_MIN_SUPPORTED,
		"rpc-version-semver": RPC_VERSION_SEMVER,
		"version": "yap_torrent (transmission-rpc compatible)",
		"download-dir": download_dir,
		"download-dir-free-space": _free_space(cfg.download_folder),
		"incomplete-dir": Path(cfg.incomplete_folder).as_posix(),
		"incomplete-dir-enabled": cfg.incomplete_folder_enabled,
		"peer-port": cfg.port,
		"peer-port-random-on-start": False,
		"port-forwarding-enabled": True,
		"dht-enabled": cfg.dht_enabled,
		"pex-enabled": True,
		"lpd-enabled": False,
		"utp-enabled": False,
		"encryption": "preferred",
		"peer-limit-global": cfg.max_connections,
		"peer-limit-per-torrent": cfg.peer_limit_per_torrent,
		"download-queue-enabled": cfg.download_queue_enabled,
		"download-queue-size": cfg.download_queue_size,
		"seed-queue-enabled": cfg.seed_queue_enabled,
		"seed-queue-size": cfg.seed_queue_size,
		"queue-stalled-enabled": False,
		"queue-stalled-minutes": STALLED_AFTER_SECONDS // 60,
		"speed-limit-down": cfg.speed_limit_down,
		"speed-limit-down-enabled": cfg.speed_limit_down_enabled,
		"speed-limit-up": cfg.speed_limit_up,
		"speed-limit-up-enabled": cfg.speed_limit_up_enabled,
		# turtle mode is ours, not core's
		"alt-speed-enabled": info.alt_speed.alt_speed_enabled,
		"alt-speed-down": info.alt_speed.alt_speed_down,
		"alt-speed-up": info.alt_speed.alt_speed_up,
		"seedRatioLimit": cfg.seed_ratio_limit,
		"seedRatioLimited": cfg.seed_ratio_limited,
		"start-added-torrents": cfg.start_added_torrents,
		"rename-partial-files": False,
		"trash-original-torrent-files": False,
		"blocklist-enabled": cfg.blocklist_enabled,
		"blocklist-url": cfg.blocklist_url,
		"blocklist-size": 0,
		"config-dir": Path(cfg.data_folder).as_posix(),
		"session-id": info.session_id,
		"units": {
			"speed-units": ["kB/s", "MB/s", "GB/s", "TB/s"],
			"speed-bytes": 1000,
			"size-units": ["kB", "MB", "GB", "TB"],
			"size-bytes": 1000,
			"memory-units": ["KiB", "MiB", "GiB", "TiB"],
			"memory-bytes": 1024,
		},
	}

	fields: Set[str] = set(arguments.get("fields", []))
	if fields:
		return "success", {key: session[key] for key in fields.intersection(session.keys())}
	return "success", session


# Transmission session key -> core config key. Anything absent here is either
# restart-only, plugin-owned, or something core has no notion of; session-set ignores
# those rather than failing the call, which is what Transmission does with unknown args.
SESSION_SETTINGS: Dict[str, str] = {
	"download-dir": "download_folder",
	"incomplete-dir": "incomplete_folder",
	"incomplete-dir-enabled": "incomplete_folder_enabled",
	"speed-limit-down": "speed_limit_down",
	"speed-limit-down-enabled": "speed_limit_down_enabled",
	"speed-limit-up": "speed_limit_up",
	"speed-limit-up-enabled": "speed_limit_up_enabled",
	"seedRatioLimit": "seed_ratio_limit",
	"seedRatioLimited": "seed_ratio_limited",
	"download-queue-enabled": "download_queue_enabled",
	"download-queue-size": "download_queue_size",
	"seed-queue-enabled": "seed_queue_enabled",
	"seed-queue-size": "seed_queue_size",
	"peer-limit-global": "max_connections",
	"peer-limit-per-torrent": "peer_limit_per_torrent",
	"blocklist-enabled": "blocklist_enabled",
	"blocklist-url": "blocklist_url",
	"start-added-torrents": "start_added_torrents",
	"dht-enabled": "dht_enabled",
	"peer-port": "port",
}


# alt-speed is ours; it maps to AltSpeed rather than to a core setting
ALT_SPEED_SETTINGS: Dict[str, str] = {
	"alt-speed-down": "alt_speed_down",
	"alt-speed-up": "alt_speed_up",
	"alt-speed-enabled": "alt_speed_enabled",
}


@method("session-set")
async def session_set(env, info, arguments):
	"""Change session settings (rpc-spec 4.1).

	Core stores several of these without acting on them (speed limits, queues,
	blocklist) — Config.set logs a warning naming each one. They are still applied
	rather than rejected so a client's choice round-trips instead of reading back as
	whatever it was before.
	"""
	alt = {
		ALT_SPEED_SETTINGS[key]: value
		for key, value in arguments.items()
		if key in ALT_SPEED_SETTINGS
	}
	if alt and info.alt_speed.update(alt):
		# our own section of config.json, since core does not model turtle mode
		env.config.set_plugin_config(PLUGIN_CONFIG_KEY, info.alt_speed.export())

	values = {
		SESSION_SETTINGS[key]: value
		for key, value in arguments.items()
		if key in SESSION_SETTINGS
	}
	if values:
		await env.event_bus.dispatch_async("request.config.set", values)
	return "success", {}


@method("session-stats")
async def session_stats(env, info, _arguments):
	entities = list(env.data_storage.get_collection(TorrentEC))
	active = sum(1 for e in entities if is_torrent_active(e))
	downloaded = sum(e.get_component(TorrentStatsEC).downloaded for e in entities)
	uploaded = sum(e.get_component(TorrentStatsEC).uploaded for e in entities)
	seconds_active = int(time.monotonic() - info.start_time)

	download_speed, upload_speed = session_rates(env)
	totals = {
		"uploadedBytes": uploaded,
		"downloadedBytes": downloaded,
		"filesAdded": len(entities),
		"sessionCount": 1,
		"secondsActive": seconds_active,
	}
	return "success", {
		"activeTorrentCount": active,
		"pausedTorrentCount": len(entities) - active,
		"torrentCount": len(entities),
		"downloadSpeed": int(download_speed),
		"uploadSpeed": int(upload_speed),
		"cumulative-stats": totals,
		"current-stats": totals,
	}


# ---------------------------------------------------------------------------
# free space (rpc-spec 4.8) and port test (rpc-spec 4.5)
# ---------------------------------------------------------------------------
def _free_space(path: Path) -> int:
	try:
		return shutil.disk_usage(path).free
	except OSError:
		return -1


@method("free-space")
async def free_space(env, _info, arguments):
	path = arguments.get("path", "")
	try:
		usage = shutil.disk_usage(Path(path) if path else env.config.download_folder)
	except OSError as ex:
		return f"free-space failed: {ex}", {}
	return "success", {"path": path, "size-bytes": usage.free, "total_size": usage.total}


@method("port-test")
async def port_test(_env, _info, _arguments):
	# TODO: no real inbound-connectivity probe is performed; this optimistically
	#  reports the configured peer port as reachable.
	return "success", {"port-is-open": True, "ipProtocol": "ipv4"}


# ---------------------------------------------------------------------------
# Recognised but not yet implemented (rpc-spec).  Returning a descriptive error
# keeps these distinct from unknown methods.
# TODO: implement each of these against the yap_torrent ECS as it grows.
# ---------------------------------------------------------------------------
UNIMPLEMENTED: Dict[str, str] = {
	# 3.6 - would need to move already-downloaded files and update TorrentPathEC.
	"torrent-set-location": "torrent-set-location is not supported yet",
	# 3.7 - no rename support in core (would rewrite TorrentInfo paths on disk).
	"torrent-rename-path": "torrent-rename-path is not supported yet",
	# 4.4 - no blocklist subsystem exists.
	"blocklist-update": "blocklist-update is not supported: no blocklist subsystem",
	# 4.6 - could set env.close_event to shut the client down; intentionally not
	#  wired to avoid remote clients killing the process by surprise.
	"session-close": "session-close is not supported",
	# 4.9 - no bandwidth groups.
	"group-get": "bandwidth groups are not supported",
	"group-set": "bandwidth groups are not supported",
}
