import asyncio
import base64
import logging
import shutil
import time
from pathlib import Path
from typing import Any, Awaitable, Callable, Dict, List, Tuple, Optional, Set

import aiohttp
from angelovich.core.DataStorage import Entity

from yap_torrent.components.file_ec import FilePriority
from yap_torrent.components.torrent_ec import (
	TorrentEC,
	TorrentQueuePositionEC,
	TorrentStatsEC,
)
from yap_torrent.config import as_bool
from yap_torrent.env import Env
from yap_torrent.protocol import decode
from yap_torrent.protocol.magnet import MagnetInfo
from yap_torrent.protocol.structures import Metainfo
from yap_torrent.settings import Setting
from yap_torrent.systems import (
	get_torrent_entity,
	get_torrent_name,
	get_local_id,
	is_torrent_active,
	iterate_torrents_in_queue_order,
)
from yap_torrent.systems.stats_system import session_rates
from .components import get_speed_settings, set_labels
from .mapping import DEFAULT_FIELDS, STALLED_AFTER_SECONDS, build_torrent
from .server_info import ServerInfo, TorrentIDs

logger = logging.getLogger(__name__)


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

Handler = Callable[[ServerInfo, Dict[str, Any]], Awaitable[Tuple[str, Dict[str, Any]]]]

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


def iterate_torrents(info: ServerInfo, ids: TorrentIDs):
	for e in info.env.data_storage.get_collection(TorrentEC):
		torrent = e.get_component(TorrentEC)
		this_id = get_local_id(e)
		if info.removed.contains(torrent, this_id):
			continue
		if not ids.empty() and not ids.contains(torrent, this_id):
			continue
		yield e


# ---------------------------------------------------------------------------
# torrent action requests (rpc-spec 3.1)
# ---------------------------------------------------------------------------
@method("torrent-start")
async def torrent_start(info, arguments):
	ids = TorrentIDs.read_ids(arguments, info.recent)
	for entity in iterate_torrents(info, ids):
		torrent = entity.get_component(TorrentEC)
		info.recent.add(get_local_id(entity))
		await info.env.event_bus.dispatch_async("request.torrent.start", torrent.info_hash)
	return "success", {}


# torrent-start-now behaves the same here; yap has no download queue to bypass.
METHODS["torrent-start-now"] = torrent_start


@method("torrent-stop")
async def torrent_stop(info, arguments):
	ids = TorrentIDs.read_ids(arguments, info.recent)
	for entity in iterate_torrents(info, ids):
		torrent = entity.get_component(TorrentEC)
		info.recent.add(get_local_id(entity))
		await info.env.event_bus.dispatch_async("request.torrent.stop", torrent.info_hash)
	return "success", {}


@method("torrent-verify")
async def torrent_verify(info, arguments):
	ids = TorrentIDs.read_ids(arguments, info.recent)
	for entity in iterate_torrents(info, ids):
		torrent = entity.get_component(TorrentEC)
		info.recent.add(get_local_id(entity))
		await info.env.event_bus.dispatch_async("request.torrent.invalidate", torrent.info_hash)
	return "success", {}


@method("torrent-reannounce")
async def torrent_reannounce(info, arguments):
	ids = TorrentIDs.read_ids(arguments, info.recent)
	for entity in iterate_torrents(info, ids):
		torrent = entity.get_component(TorrentEC)
		info.recent.add(get_local_id(entity))
		await info.env.event_bus.dispatch_async("request.torrent.reannounce", torrent.info_hash)
	return "success", {}


# ---------------------------------------------------------------------------
# per-torrent settings (rpc-spec 3.2)
# ---------------------------------------------------------------------------
# Transmission is lenient about arguments it cannot act on, and failing the whole call
# over one unsupported key would break clients that always send a full settings block.
# Still inert: "location" (no move-on-disk in core) and the deprecated trackerAdd /
# trackerRemove / trackerReplace. The bandwidth and ratio keys below ARE accepted and
# stored, but nothing enforces them yet — see TorrentLimitsEC.
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


def _move_target(direction: Any, index: int, count: int) -> Optional[int]:
	"""Where a move puts a torrent: one of top/up/down/bottom, or an absolute position.

	This is Transmission's vocabulary, not core's — core holds a queue of ordinals and is
	handed the finished order. Both the meaning of a direction and the clamping of a
	position out of range are decided here, including that a JSON `true` is not position 1
	(`bool` is an int subclass, so the int branch would otherwise swallow it).
	"""
	if isinstance(direction, bool):
		return None
	if isinstance(direction, int):
		return max(0, min(count - 1, direction))

	targets = {"top": 0, "bottom": count - 1, "up": index - 1, "down": index + 1}
	if direction not in targets:
		return None
	return max(0, min(count - 1, targets[direction]))


def _queued(env: Env) -> List[Entity]:
	"""The torrents holding a queue place, in queue order."""
	return [entity for entity in iterate_torrents_in_queue_order(env)
	        if entity.has_component(TorrentQueuePositionEC)]


async def _move_in_queue(env: Env, targets: List[Entity], direction: Any) -> None:
	"""Reinsert each of `targets` where `direction` puts it, then hand core the new order."""
	ordered = _queued(env)
	moved = False

	for entity in targets:
		if entity not in ordered:
			continue
		index = ordered.index(entity)
		target = _move_target(direction, index, len(ordered))
		if target is None:
			logger.warning("Ignoring invalid queue position/direction: %r", direction)
			return
		if target == index:
			continue
		ordered.insert(target, ordered.pop(index))
		moved = True

	if not moved:
		return
	await env.event_bus.dispatch_async(
		"request.torrent.queue_order", [e.get_component(TorrentEC).info_hash for e in ordered])


async def _apply_torrent_settings(info: ServerInfo, torrent_entity: Entity, arguments: Dict[str, Any]):
	"""Apply every torrent-set-shaped argument. Shared with torrent-add."""
	info_hash = torrent_entity.get_component(TorrentEC).info_hash

	# no default: absent means "leave them alone", [] means "clear them"
	labels = arguments.get("labels")
	if labels is not None:
		# ours end to end: core has no label concept, so they live in its custom_data
		set_labels(torrent_entity, info.name, labels)

	# an empty list means "all files" to Transmission, which is also what
	# request.file.select reads None as
	wanted = arguments.get("files-wanted")
	if wanted is not None:
		await info.env.event_bus.dispatch_async("request.file.select", info_hash, wanted or None, True, None)
	unwanted = arguments.get("files-unwanted")
	if unwanted is not None:
		await info.env.event_bus.dispatch_async("request.file.select", info_hash, unwanted or None, False, None)

	for argument, priority in _FILE_PRIORITIES.items():
		indices = arguments.get(argument)
		if indices is not None:
			await info.env.event_bus.dispatch_async(
				"request.file.select", info_hash, indices or None, None, priority)

	limits = {field: arguments[key] for key, field in _LIMIT_ARGUMENTS.items() if key in arguments}
	if limits:
		await info.env.event_bus.dispatch_async("request.torrent.set_limits", info_hash, limits)

	if arguments.get("queuePosition") is not None:
		await _move_in_queue(info.env, [torrent_entity], arguments["queuePosition"])


@method("torrent-set")
async def torrent_set(info, arguments):
	ids = TorrentIDs.read_ids(arguments, info.recent)
	for entity in iterate_torrents(info, ids):
		torrent = entity.get_component(TorrentEC)
		info.recent.add(get_local_id(entity))
		await _apply_torrent_settings(info, entity, arguments)
	return "success", {}


# ---------------------------------------------------------------------------
# queue movement (rpc-spec 4.7)
# ---------------------------------------------------------------------------
def _queue_mover(direction: str) -> Handler:
	async def handler(info, arguments):
		ids = TorrentIDs.read_ids(arguments, info.recent)
		# The order a multi-torrent selection is applied in decides whether it keeps its
		# relative order or is turned inside out, and the two kinds of move want opposite
		# orders. For an end jump, whichever is applied last ends up nearest that end — so
		# "top" runs backwards and "bottom" forwards. For a single step, each torrent has
		# to move before the neighbour it is stepping past does — so "up" runs forwards
		# (nearest the top first) and "down" backwards.
		entities = list(iterate_torrents(info, ids))
		if direction in ("top", "down"):
			entities.reverse()
		for entity in entities:
			info.recent.add(get_local_id(entity))
		await _move_in_queue(info.env, entities, direction)
		return "success", {}

	return handler


for _direction in ("top", "up", "down", "bottom"):
	METHODS[f"queue-move-{_direction}"] = _queue_mover(_direction)


# ---------------------------------------------------------------------------
# removing torrents (rpc-spec 3.5)
# ---------------------------------------------------------------------------
@method("torrent-remove")
async def torrent_remove(info: ServerInfo, arguments):
	delete_data = bool(arguments.get("delete-local-data", False))
	ids = TorrentIDs.read_ids(arguments, info.recent)
	for entity in iterate_torrents(info, ids):
		torrent = entity.get_component(TorrentEC)
		logger.info("[torrent-remove] remove torrent: %s (delete_data=%s)", get_torrent_name(entity), delete_data)

		info.removed.add(get_local_id(entity))
		info.env.event_bus.dispatch("request.torrent.remove", torrent.info_hash, delete_data)

	return "success", {}


# ---------------------------------------------------------------------------
# torrent accessors (rpc-spec 3.3)
# ---------------------------------------------------------------------------
@method("torrent-get")
async def torrent_get(info, arguments):
	fields = arguments.get("fields") or list(DEFAULT_FIELDS)
	table = arguments.get("format") == "table"

	ids = TorrentIDs.read_ids(arguments, info.recent)
	torrents = [build_torrent(e, fields, info) for e in iterate_torrents(info, ids)]

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
async def torrent_add(info, arguments):
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
		return await _add_metainfo(info, data, download_dir, paused, arguments)

	if filename:
		if filename.startswith("magnet:"):
			return await _add_magnet(info, filename, paused)
		if filename.startswith("http://") or filename.startswith("https://"):
			data = await fetch_url(filename)
			if data is None:
				return "download of torrent file failed", {}
			return await _add_metainfo(info, data, download_dir, paused, arguments)
		# Otherwise treat it as a local .torrent file path.
		try:
			with open(filename, "rb") as handle:
				data = handle.read()
		except OSError as ex:
			return f"unable to read torrent file: {ex}", {}
		return await _add_metainfo(info, data, download_dir, paused, arguments)

	return 'either "filename" or "metainfo" must be included', {}


def _added_stub(env: Env, entity: Entity) -> Dict[str, Any]:
	return {
		"id": get_local_id(entity),
		"hashString": entity.get_component(TorrentEC).info_hash.hex(),
		"name": get_torrent_name(entity),
	}


async def _add_metainfo(info: ServerInfo, data: bytes, download_dir: Optional[Path], paused: bool,
                        arguments: Dict[str, Any]):
	env = info.env
	try:
		file_info = Metainfo(decode(data))
		info_hash = file_info.make_info_hash()
	except Exception as ex:  # noqa: BLE001 - any parse failure is a client error
		return f"invalid or corrupt torrent metainfo: {ex}", {}

	existing = get_torrent_entity(env, info_hash)
	if existing is not None:
		return "success", {"torrent-duplicate": _added_stub(env, existing)}

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
	await _apply_torrent_settings(info, torrent_entity, arguments)

	info.recent.add(get_local_id(torrent_entity))

	logger.info("torrent-add: added %s", info_hash.hex())
	return "success", {"torrent-added": _added_stub(env, torrent_entity)}


async def _add_magnet(info: ServerInfo, magnet_link: str, paused):
	env = info.env
	magnet = MagnetInfo(magnet_link)
	if not magnet.is_valid():
		return "invalid magnet link", {}

	info_hash = magnet.info_hash
	existing = get_torrent_entity(env, info_hash)
	if existing is not None:
		return "success", {"torrent-duplicate": _added_stub(env, existing)}

	await env.event_bus.dispatch_async("request.magnet.add", magnet_link)

	torrent_entity = get_torrent_entity(env, info_hash)
	if not torrent_entity:
		return "add magnet operation failed", {}

	if paused:
		env.event_bus.dispatch("request.torrent.stop", info_hash)

	info.recent.add(get_local_id(torrent_entity))

	# a magnet has no metadata yet, so prefer its display name (dn) over the
	# entity's placeholder name until real metadata arrives
	stub = _added_stub(env, torrent_entity)
	if magnet.name:
		stub["name"] = magnet.name

	logger.info("torrent-add: queued magnet %s, paused %s", info_hash.hex(), paused)
	return "success", {"torrent-added": stub}


# ---------------------------------------------------------------------------
# session accessors (rpc-spec 4.2 / 4.3)
# ---------------------------------------------------------------------------
@method("session-get")
async def session_get(info, arguments):
	env = info.env
	cfg = env.config
	speed = get_speed_settings(env)
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
		# core stores one number per direction where 0 means no limit; a client wants the
		# number and the switch separately, and expects its number to still be in the box
		# after it switches the limit off
		"speed-limit-down": speed.reported_limit(cfg.speed_limit_down, "last_speed_limit_down"),
		"speed-limit-down-enabled": bool(cfg.speed_limit_down),
		"speed-limit-up": speed.reported_limit(cfg.speed_limit_up, "last_speed_limit_up"),
		"speed-limit-up-enabled": bool(cfg.speed_limit_up),
		# turtle mode is ours, not core's
		"alt-speed-enabled": speed.alt_speed_enabled,
		"alt-speed-down": speed.alt_speed_down,
		"alt-speed-up": speed.alt_speed_up,
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


# The config properties this plugin offers its clients, with the cast each needs to read a
# JSON value, and — where nothing acts on the value yet — why. Core holds the properties
# and knows none of this: what is worth exposing, and what a client is allowed to send for
# it, is ours. Registered at start-up by `RpcServer`.
_NOT_ENFORCED_BANDWIDTH = "bandwidth limiting is not implemented; the value is stored and reported only"
_NOT_ENFORCED_QUEUE = "there is no active-torrent queue; the value is stored and reported only"
_NOT_ENFORCED_PEERS = "connection admission is driven by the queue limits; the value is stored and reported only"
_NOT_ENFORCED_RATIO = "seeding is never stopped on ratio; the value is stored and reported only"
_NOT_ENFORCED_INCOMPLETE = "downloads are written straight to download_folder"
_NOT_ENFORCED_BLOCKLIST = "no blocklist subsystem; no peer is ever filtered"

CORE_SETTINGS: Tuple[Setting, ...] = (
	Setting("download_folder", Path),
	Setting("port", int),
	Setting("dht_enabled", as_bool),
	Setting("start_added_torrents", as_bool),

	Setting("incomplete_folder", Path, note=_NOT_ENFORCED_INCOMPLETE),
	Setting("incomplete_folder_enabled", as_bool, note=_NOT_ENFORCED_INCOMPLETE),
	Setting("speed_limit_down", int, note=_NOT_ENFORCED_BANDWIDTH),
	Setting("speed_limit_up", int, note=_NOT_ENFORCED_BANDWIDTH),
	Setting("seed_ratio_limit", float, note=_NOT_ENFORCED_RATIO),
	Setting("seed_ratio_limited", as_bool, note=_NOT_ENFORCED_RATIO),
	Setting("download_queue_enabled", as_bool, note=_NOT_ENFORCED_QUEUE),
	Setting("download_queue_size", int, note=_NOT_ENFORCED_QUEUE),
	Setting("seed_queue_enabled", as_bool, note=_NOT_ENFORCED_QUEUE),
	Setting("seed_queue_size", int, note=_NOT_ENFORCED_QUEUE),
	Setting("max_connections", int, note=_NOT_ENFORCED_PEERS),
	Setting("peer_limit_per_torrent", int, note=_NOT_ENFORCED_PEERS),
	Setting("blocklist_enabled", as_bool, note=_NOT_ENFORCED_BLOCKLIST),
	Setting("blocklist_url", str, note=_NOT_ENFORCED_BLOCKLIST),
)

# Transmission session key -> core config key. Anything absent here is either
# plugin-owned or something core has no notion of; session-set ignores those rather than
# failing the call, which is what Transmission does with unknown args.
SESSION_SETTINGS: Dict[str, str] = {
	"download-dir": "download_folder",
	"incomplete-dir": "incomplete_folder",
	"incomplete-dir-enabled": "incomplete_folder_enabled",
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

# alt-speed is ours; it maps to SpeedSettingsEC rather than to a core setting
ALT_SPEED_SETTINGS: Dict[str, str] = {
	"alt-speed-down": "alt_speed_down",
	"alt-speed-up": "alt_speed_up",
	"alt-speed-enabled": "alt_speed_enabled",
}

# the (number, flag) pairs that fold into core's single "0 means no limit"
_SPEED_LIMITS = (
	("speed-limit-down", "speed-limit-down-enabled", "speed_limit_down", "last_speed_limit_down"),
	("speed-limit-up", "speed-limit-up-enabled", "speed_limit_up", "last_speed_limit_up"),
)


@method("session-set")
async def session_set(info, arguments):
	"""Change session settings (rpc-spec 4.1).

	Core stores several of these without acting on them (speed limits, queues,
	blocklist) — the note on each registered Setting says so, and applying one logs it.
	They are still applied rather than rejected so a client's choice round-trips instead
	of reading back as whatever it was before.
	"""
	# turtle mode and the remembered numbers are live state, not stored preferences:
	# they change the component and nothing else, and are gone with the process
	env = info.env
	speed = get_speed_settings(env)
	speed.update({
		ALT_SPEED_SETTINGS[key]: value
		for key, value in arguments.items()
		if key in ALT_SPEED_SETTINGS
	})

	values = {
		SESSION_SETTINGS[key]: value
		for key, value in arguments.items()
		if key in SESSION_SETTINGS
	}

	# each limit arrives as a number plus a flag; core holds one number where 0 is off
	for value_key, flag_key, config_key, remembered_attr in _SPEED_LIMITS:
		if value_key not in arguments and flag_key not in arguments:
			continue
		resolved = speed.resolve_limit(
			getattr(env.config, config_key), remembered_attr,
			arguments.get(value_key), arguments.get(flag_key))
		if resolved is not None:
			values[config_key] = resolved

	for key, value in values.items():
		await env.event_bus.dispatch_async("request.setting.apply", key, value)
	return "success", {}


@method("session-stats")
async def session_stats(info, _arguments):
	env = info.env
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
async def free_space(info, arguments):
	path = arguments.get("path", "")
	try:
		usage = shutil.disk_usage(Path(path) if path else info.env.config.download_folder)
	except OSError as ex:
		return f"free-space failed: {ex}", {}
	return "success", {"path": path, "size-bytes": usage.free, "total_size": usage.total}


@method("port-test")
async def port_test(_info, _arguments):
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
