"""Transmission RPC method handlers.

Each handler has the signature ``async def handler(server, arguments) -> (result, args)``
where ``result`` is the Transmission result string (``"success"`` on success, any other
string is treated as an error by clients) and ``args`` is the ``arguments`` object of the
response.

Implemented methods are registered with the :func:`method` decorator.  Every other method
named by the spec is listed in :data:`UNIMPLEMENTED` so it is *recognised* (returns an
explanatory error) rather than reported as an unknown method.
"""
import base64
import logging
import shutil
import time
from pathlib import Path
from typing import Any, Awaitable, Callable, Dict, List, Tuple

from yap_torrent.components.torrent_ec import (
	SaveTorrentEC,
	TorrentEC,
	TorrentStatsEC,
	ValidateTorrentEC,
)
from yap_torrent.components.tracker_ec import TorrentTrackerDataEC, TorrentTrackerEC
from yap_torrent.protocol import decode
from yap_torrent.protocol.magnet import MagnetInfo
from yap_torrent.protocol.structures import TorrentFileInfo
from yap_torrent.systems import create_torrent_entity, get_torrent_entity, get_torrent_name, is_torrent_active

from .mapping import DEFAULT_FIELDS, build_torrent

logger = logging.getLogger(__name__)

Handler = Callable[[Any, Dict[str, Any]], Awaitable[Tuple[str, Dict[str, Any]]]]

METHODS: Dict[str, Handler] = {}


def method(name: str) -> Callable[[Handler], Handler]:
	def register(func: Handler) -> Handler:
		METHODS[name] = func
		return func

	return register


# ---------------------------------------------------------------------------
# id resolution
# ---------------------------------------------------------------------------
def resolve_hashes(server, arguments: Dict[str, Any]) -> List[bytes]:
	"""Resolve the Transmission ``ids`` argument to a list of info_hash bytes.

	Accepts an integer id, a 40-char hex hashString, a list mixing both, the
	string ``"recently-active"``, or nothing at all (meaning "all torrents").
	"""
	ds = server.env.data_storage
	all_hashes = [e.get_component(TorrentEC).info_hash for e in ds.get_collection(TorrentEC)]

	ids_arg = arguments.get("ids", None)
	# "recently-active" (and the spec's alternate spelling) -> all torrents.
	# TODO: track a real recently-active set (torrents touched since the last
	#  such request) instead of returning everything.
	if ids_arg is None or ids_arg in ("recently-active", "recently_active"):
		return all_hashes

	if not isinstance(ids_arg, list):
		ids_arg = [ids_arg]

	hashes: List[bytes] = []
	for ref in ids_arg:
		if isinstance(ref, bool):
			continue
		if isinstance(ref, int):
			info_hash_hex = server.ids.hash_for_id(ref)
			if info_hash_hex:
				hashes.append(bytes.fromhex(info_hash_hex))
		elif isinstance(ref, str):
			try:
				hashes.append(bytes.fromhex(ref))
			except ValueError:
				logger.warning("ignoring unparseable torrent id %r", ref)
	return hashes


# ---------------------------------------------------------------------------
# torrent action requests (rpc-spec 3.1)
# ---------------------------------------------------------------------------
@method("torrent-start")
async def torrent_start(server, arguments):
	for info_hash in resolve_hashes(server, arguments):
		server.env.event_bus.dispatch("request.torrent.start", info_hash)
	return "success", {}


# torrent-start-now behaves the same here; yap has no download queue to bypass.
METHODS["torrent-start-now"] = torrent_start


@method("torrent-stop")
async def torrent_stop(server, arguments):
	for info_hash in resolve_hashes(server, arguments):
		server.env.event_bus.dispatch("request.torrent.stop", info_hash)
	return "success", {}


@method("torrent-verify")
async def torrent_verify(server, arguments):
	# Maps to the ECS "invalidate" request which re-checks piece hashes on disk.
	for info_hash in resolve_hashes(server, arguments):
		server.env.event_bus.dispatch("request.torrent.invalidate", info_hash)
	return "success", {}


# ---------------------------------------------------------------------------
# removing torrents (rpc-spec 3.5)
# ---------------------------------------------------------------------------
@method("torrent-remove")
async def torrent_remove(server, arguments):
	# TODO: the "delete-local-data" argument is not honoured. Core removal
	#  (request.torrent.remove) leaves downloaded files on disk; deleting them
	#  would need to walk TorrentInfo.files under the torrent path.
	for info_hash in resolve_hashes(server, arguments):
		server.env.event_bus.dispatch("request.torrent.remove", info_hash)
	return "success", {}


# ---------------------------------------------------------------------------
# torrent accessors (rpc-spec 3.3)
# ---------------------------------------------------------------------------
@method("torrent-get")
async def torrent_get(server, arguments):
	fields = arguments.get("fields") or list(DEFAULT_FIELDS)
	# TODO: the "table" format is not supported; results are always objects.
	wanted = None
	if arguments.get("ids") is not None:
		wanted = set(resolve_hashes(server, arguments))

	torrents = []
	for entity in server.env.data_storage.get_collection(TorrentEC):
		info_hash = entity.get_component(TorrentEC).info_hash
		if wanted is not None and info_hash not in wanted:
			continue
		torrents.append(build_torrent(entity, fields, server.ids, server.env))
	return "success", {"torrents": torrents}


# ---------------------------------------------------------------------------
# adding a torrent (rpc-spec 3.4)
# ---------------------------------------------------------------------------
@method("torrent-add")
async def torrent_add(server, arguments):
	download_dir = arguments.get("download-dir")
	paused = bool(arguments.get("paused", False))
	metainfo = arguments.get("metainfo")
	filename = arguments.get("filename")

	if metainfo:
		try:
			data = base64.b64decode(metainfo)
		except (ValueError, TypeError) as ex:
			return f"invalid metainfo: {ex}", {}
		return _add_metainfo(server, data, download_dir, paused)

	if filename:
		if filename.startswith("magnet:"):
			return _add_magnet(server, filename)
		if filename.startswith("http://") or filename.startswith("https://"):
			data = await server.fetch_url(filename)
			if data is None:
				return "download of torrent file failed", {}
			return _add_metainfo(server, data, download_dir, paused)
		# Otherwise treat it as a local .torrent file path.
		try:
			with open(filename, "rb") as handle:
				data = handle.read()
		except OSError as ex:
			return f"unable to read torrent file: {ex}", {}
		return _add_metainfo(server, data, download_dir, paused)

	return 'either "filename" or "metainfo" must be included', {}


def _added_stub(server, entity) -> Dict[str, Any]:
	info_hash_hex = entity.get_component(TorrentEC).info_hash.hex()
	return {
		"id": server.ids.id_for_hash(info_hash_hex),
		"hashString": info_hash_hex,
		"name": get_torrent_name(entity),
	}


def _add_metainfo(server, data: bytes, download_dir, paused: bool):
	try:
		file_info = TorrentFileInfo(decode(data))
		info_hash = file_info.make_info_hash()
	except Exception as ex:  # noqa: BLE001 - any parse failure is a client error
		return f"invalid or corrupt torrent metainfo: {ex}", {}

	env = server.env
	existing = get_torrent_entity(env, info_hash)
	if existing is not None:
		return "success", {"torrent-duplicate": _added_stub(server, existing)}

	# Mirror WatcherSystem's add path (no request.* event exists for metainfo).
	path = Path(download_dir) if download_dir else Path(env.config.download_folder)
	entity = create_torrent_entity(env, info_hash, path, {}, file_info.info)

	announce_list = file_info.announce_list
	if announce_list:
		entity.add_component(TorrentTrackerEC(announce_list))
		entity.add_component(TorrentTrackerDataEC())

	entity.add_component(SaveTorrentEC())
	entity.add_component(ValidateTorrentEC())

	if paused:
		env.event_bus.dispatch("request.torrent.stop", info_hash)

	logger.info("torrent-add: added %s", info_hash.hex())
	return "success", {"torrent-added": _added_stub(server, entity)}


def _add_magnet(server, magnet_link: str):
	magnet = MagnetInfo(magnet_link)
	if not magnet.is_valid():
		return "invalid magnet link", {}

	env = server.env
	info_hash = magnet.info_hash
	existing = get_torrent_entity(env, info_hash)
	if existing is not None:
		return "success", {"torrent-duplicate": _added_stub(server, existing)}

	# Idiomatic magnet path: the MagnetSystem creates the entity asynchronously.
	# TODO: the "paused" flag is ignored for magnets because the entity does not
	#  exist yet when this returns; pausing would race the MagnetSystem.
	env.event_bus.dispatch("request.magnet.add", magnet_link)

	info_hash_hex = info_hash.hex()
	stub = {
		"id": server.ids.id_for_hash(info_hash_hex),
		"hashString": info_hash_hex,
		"name": magnet.name or info_hash_hex,
	}
	logger.info("torrent-add: queued magnet %s", info_hash_hex)
	return "success", {"torrent-added": stub}


# ---------------------------------------------------------------------------
# session accessors (rpc-spec 4.2 / 4.3)
# ---------------------------------------------------------------------------
@method("session-get")
async def session_get(server, arguments):
	cfg = server.env.config
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
		"session-id": server.session_id,
		"units": {
			"speed-units": ["kB/s", "MB/s", "GB/s", "TB/s"],
			"speed-bytes": 1000,
			"size-units": ["kB", "MB", "GB", "TB"],
			"size-bytes": 1000,
			"memory-units": ["KiB", "MiB", "GiB", "TiB"],
			"memory-bytes": 1024,
		},
	}
	fields = arguments.get("fields")
	if fields:
		return "success", {key: session[key] for key in fields if key in session}
	return "success", session


@method("session-stats")
async def session_stats(server, arguments):
	entities = list(server.env.data_storage.get_collection(TorrentEC))
	active = sum(1 for e in entities if is_torrent_active(e))
	downloaded = sum(e.get_component(TorrentStatsEC).downloaded for e in entities)
	uploaded = sum(e.get_component(TorrentStatsEC).uploaded for e in entities)
	seconds_active = int(time.monotonic() - server.start_time)

	# TODO: downloadSpeed / uploadSpeed are 0 because core has no rate metering.
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
		"downloadSpeed": 0,
		"uploadSpeed": 0,
		"cumulative-stats": totals,
		"current-stats": totals,
	}


# ---------------------------------------------------------------------------
# free space (rpc-spec 4.8) and port test (rpc-spec 4.5)
# ---------------------------------------------------------------------------
def _free_space(path: str) -> int:
	try:
		return shutil.disk_usage(path).free
	except OSError:
		return -1


@method("free-space")
async def free_space(server, arguments):
	path = arguments.get("path") or server.env.config.download_folder
	try:
		usage = shutil.disk_usage(path)
	except OSError as ex:
		return f"free-space failed: {ex}", {}
	return "success", {"path": path, "size-bytes": usage.free, "total_size": usage.total}


@method("port-test")
async def port_test(server, arguments):
	# TODO: no real inbound-connectivity probe is performed; this optimistically
	#  reports the configured peer port as reachable.
	return "success", {"port-is-open": True, "ipProtocol": "ipv4"}


# ---------------------------------------------------------------------------
# Recognised but not yet implemented (rpc-spec).  Returning a descriptive error
# keeps these distinct from unknown methods.
# TODO: implement each of these against the yap_torrent ECS as it grows.
# ---------------------------------------------------------------------------
UNIMPLEMENTED: Dict[str, str] = {
	# 3.2 - needs settable per-torrent properties (bandwidth caps, file
	#  wanted/priority, labels, tracker edits) which the ECS does not model.
	"torrent-set": "torrent-set is not supported: per-torrent mutable settings are not modelled yet",
	# 3.1 - no reannounce request exists; AnnounceSystem drives announces on its
	#  own timer. Needs a "request.torrent.reannounce" event to force one.
	"torrent-reannounce": "torrent-reannounce is not supported yet",
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
	# 4.7 - yap has no download/seed queue, so queue movement is a no-op concept.
	"queue-move-top": "queue movement is not supported: no queue subsystem",
	"queue-move-up": "queue movement is not supported: no queue subsystem",
	"queue-move-down": "queue movement is not supported: no queue subsystem",
	"queue-move-bottom": "queue movement is not supported: no queue subsystem",
	# 4.9 - no bandwidth groups.
	"group-get": "bandwidth groups are not supported",
	"group-set": "bandwidth groups are not supported",
}
