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
from .mapping import DEFAULT_FIELDS, build_torrent

logger = logging.getLogger(__name__)

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


class ServerInfo:
	def __init__(self, session_id, start_time):
		self.session_id: str = session_id
		self.start_time: float = start_time
		self.recent: TorrentIDs = TorrentIDs()
		self.removed: TorrentIDs = TorrentIDs()


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
# Only the sub-arguments core can honour are applied; Transmission is lenient about
# the rest, and silently accepting an argument we cannot act on is friendlier to
# clients than failing the whole call. Still unsupported: downloadLimit /
# uploadLimit / honorsSessionLimits (no bandwidth limiting), seedRatioLimit /
# seedRatioMode (no ratio tracking), trackerAdd / trackerRemove / trackerReplace.
_FILE_PRIORITIES = {
	"priority-high": FilePriority.High,
	"priority-normal": FilePriority.Normal,
	"priority-low": FilePriority.Low,
}


@method("torrent-set")
async def torrent_set(env, info, arguments):
	ids = TorrentIDs.read_ids(arguments, info.recent)
	labels = arguments.get("labels")
	wanted = arguments.get("files-wanted")
	unwanted = arguments.get("files-unwanted")

	for entity in iterate_torrents(env, ids, info.removed):
		torrent = entity.get_component(TorrentEC)
		info.recent.add(torrent.index)

		if labels is not None:
			await env.event_bus.dispatch_async("request.torrent.set_labels", torrent.info_hash, labels)

		# an empty list means "all files" to Transmission, which is also what
		# request.file.select reads None as
		if wanted is not None:
			await env.event_bus.dispatch_async(
				"request.file.select", torrent.info_hash, wanted or None, True, None)
		if unwanted is not None:
			await env.event_bus.dispatch_async(
				"request.file.select", torrent.info_hash, unwanted or None, False, None)

		for argument, priority in _FILE_PRIORITIES.items():
			indices = arguments.get(argument)
			if indices is None:
				continue
			await env.event_bus.dispatch_async(
				"request.file.select", torrent.info_hash, indices or None, None, priority)

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
	# TODO: the "table" format is not supported; results are always objects.

	ids = TorrentIDs.read_ids(arguments, info.recent)
	result: Dict[str, Any] = {
		"torrents": [build_torrent(e, fields, env) for e in iterate_torrents(env, ids, info.removed)]
	}

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
		return await _add_metainfo(env, info, data, download_dir, paused)

	if filename:
		if filename.startswith("magnet:"):
			return await _add_magnet(env, info, filename, paused)
		if filename.startswith("http://") or filename.startswith("https://"):
			data = await fetch_url(filename)
			if data is None:
				return "download of torrent file failed", {}
			return await _add_metainfo(env, info, data, download_dir, paused)
		# Otherwise treat it as a local .torrent file path.
		try:
			with open(filename, "rb") as handle:
				data = handle.read()
		except OSError as ex:
			return f"unable to read torrent file: {ex}", {}
		return await _add_metainfo(env, info, data, download_dir, paused)

	return 'either "filename" or "metainfo" must be included', {}


def _added_stub(entity: Entity) -> Dict[str, Any]:
	return {
		"id": entity.get_component(TorrentEC).index,
		"hashString": entity.get_component(TorrentEC).info_hash.hex(),
		"name": get_torrent_name(entity),
	}


async def _add_metainfo(env: Env, info: ServerInfo, data: bytes, download_dir: Optional[Path], paused: bool):
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
		"rpc-version": 17,
		"rpc-version-minimum": 14,
		"rpc-version-semver": "5.4.0",
		"version": "yap_torrent (transmission-rpc compatible)",
		"download-dir": download_dir,
		"download-dir-free-space": _free_space(cfg.download_folder),
		"incomplete-dir": download_dir,
		"incomplete-dir-enabled": False,
		"peer-port": cfg.port,
		"peer-port-random-on-start": False,
		"port-forwarding-enabled": True,
		"dht-enabled": True,
		"pex-enabled": True,
		"lpd-enabled": False,
		"utp-enabled": False,
		"encryption": "preferred",
		"peer-limit-global": cfg.max_connections,
		"peer-limit-per-torrent": cfg.max_connections,
		"download-queue-enabled": False,
		"download-queue-size": 0,
		"seed-queue-enabled": False,
		"seed-queue-size": 0,
		"queue-stalled-enabled": False,
		"queue-stalled-minutes": 30,
		"speed-limit-down": 0,
		"speed-limit-down-enabled": False,
		"speed-limit-up": 0,
		"speed-limit-up-enabled": False,
		"alt-speed-enabled": False,
		"alt-speed-down": 0,
		"alt-speed-up": 0,
		"seedRatioLimit": 0.0,
		"seedRatioLimited": False,
		"start-added-torrents": True,
		"rename-partial-files": False,
		"trash-original-torrent-files": False,
		"blocklist-enabled": False,
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
	# 4.1 - session is configured via config.json; runtime mutation is not wired.
	"session-set": "session-set is not supported: settings come from config.json",
	# 4.4 - no blocklist subsystem exists.
	"blocklist-update": "blocklist-update is not supported: no blocklist subsystem",
	# 4.6 - could set env.close_event to shut the client down; intentionally not
	#  wired to avoid remote clients killing the process by surprise.
	"session-close": "session-close is not supported",
	# 4.9 - no bandwidth groups.
	"group-get": "bandwidth groups are not supported",
	"group-set": "bandwidth groups are not supported",
}
