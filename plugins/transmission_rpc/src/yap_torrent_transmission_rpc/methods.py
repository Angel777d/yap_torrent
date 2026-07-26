import asyncio
import base64
import logging
import shutil
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Awaitable, Callable, Dict, List, Tuple, Optional, Set

import aiohttp
from angelovich.core.DataStorage import DataStorage, Entity

from yap_torrent.components.torrent_ec import (
	TorrentEC,
	TorrentStatsEC,
)
from yap_torrent.env import Env
from yap_torrent.protocol import decode
from yap_torrent.protocol.magnet import MagnetInfo
from yap_torrent.protocol.structures import Metainfo
from yap_torrent.systems import get_torrent_entity, get_torrent_name, is_torrent_active
from .mapping import DEFAULT_FIELDS, build_torrent

logger = logging.getLogger(__name__)


@dataclass(slots=True)
class ServerInfo:
	session_id: str
	start_time: float


Handler = Callable[[Env, ServerInfo, Dict[str, Any]], Awaitable[Tuple[str, Dict[str, Any]]]]

METHODS: Dict[str, Handler] = {}


def method(name: str) -> Callable[[Handler], Handler]:
	def register(func: Handler) -> Handler:
		METHODS[name] = func
		return func

	return register


# ---------------------------------------------------------------------------
# id resolution
# ---------------------------------------------------------------------------
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


def get_all_hashes(ds: DataStorage) -> List[bytes]:
	"""Return all info_hash bytes in the data storage."""
	return [e.get_component(TorrentEC).info_hash for e in ds.get_collection(TorrentEC)]


def get_recent(ds: DataStorage) -> List[bytes]:
	# "recently-active" (and the spec's alternate spelling) -> all torrents.
	# TODO: track a real recently active set (torrents touched since the last
	#  such request) instead of returning everything.

	return get_all_hashes(ds)


def index_to_info_hash(ds: DataStorage, index: int) -> Optional[bytes]:
	for e in ds.get_collection(TorrentEC):
		torrent: TorrentEC = e.get_component(TorrentEC)
		if torrent.index == index:
			return torrent.info_hash
	return None


def resolve_hashes(ds: DataStorage, arguments: Dict[str, Any]) -> List[bytes]:
	"""Resolve the Transmission ``ids`` argument to a list of info_hash bytes.

	Accepts an integer id, a 40-char hex hashString, a list mixing both, the
	string ``"recently-active"``, or nothing at all (meaning "all torrents").
	"""

	ids_arg = arguments.get("ids", None)
	if ids_arg is None or ids_arg in ("recently-active", "recently_active"):
		return get_recent(ds)

	if not isinstance(ids_arg, list):
		ids_arg = [ids_arg]

	hashes: List[bytes] = []
	for ref in ids_arg:
		if isinstance(ref, int):
			info_hash = index_to_info_hash(ds, ref)
			if info_hash:
				hashes.append(info_hash)
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
async def torrent_start(env, info, arguments):
	for info_hash in resolve_hashes(env.data_storage, arguments):
		env.event_bus.dispatch("request.torrent.start", info_hash)
	return "success", {}


# torrent-start-now behaves the same here; yap has no download queue to bypass.
METHODS["torrent-start-now"] = torrent_start


@method("torrent-stop")
async def torrent_stop(env, info, arguments):
	for info_hash in resolve_hashes(env.data_storage, arguments):
		env.event_bus.dispatch("request.torrent.stop", info_hash)
	return "success", {}


@method("torrent-verify")
async def torrent_verify(env, info, arguments):
	# Maps to the ECS "invalidate" request which re-checks piece hashes on disk.
	for info_hash in resolve_hashes(env.data_storage, arguments):
		env.event_bus.dispatch("request.torrent.invalidate", info_hash)
	return "success", {}


# ---------------------------------------------------------------------------
# removing torrents (rpc-spec 3.5)
# ---------------------------------------------------------------------------
@method("torrent-remove")
async def torrent_remove(env, info, arguments):
	for info_hash in resolve_hashes(env.data_storage, arguments):
		if arguments.get("delete-local-data"):
			# TODO: support "delete-local-data" in torrent app
			env.event_bus.dispatch("request.torrent.files.remove", info_hash)

		env.event_bus.dispatch("request.torrent.remove", info_hash)

	return "success", {}


# ---------------------------------------------------------------------------
# torrent accessors (rpc-spec 3.3)
# ---------------------------------------------------------------------------
@method("torrent-get")
async def torrent_get(env, info, arguments):
	fields = arguments.get("fields") or list(DEFAULT_FIELDS)
	# TODO: the "table" format is not supported; results are always objects.
	wanted = None
	if arguments.get("ids") is not None:
		wanted = set(resolve_hashes(env.data_storage, arguments))

	torrents = []
	for entity in env.data_storage.get_collection(TorrentEC):
		info_hash = entity.get_component(TorrentEC).info_hash
		if wanted is not None and info_hash not in wanted:
			continue
		torrents.append(build_torrent(entity, fields, env))
	return "success", {"torrents": torrents}


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
		return await _add_metainfo(env, data, download_dir, paused)

	if filename:
		if filename.startswith("magnet:"):
			return await _add_magnet(env, filename, paused)
		if filename.startswith("http://") or filename.startswith("https://"):
			data = await fetch_url(filename)
			if data is None:
				return "download of torrent file failed", {}
			return await _add_metainfo(env, data, download_dir, paused)
		# Otherwise treat it as a local .torrent file path.
		try:
			with open(filename, "rb") as handle:
				data = handle.read()
		except OSError as ex:
			return f"unable to read torrent file: {ex}", {}
		return await _add_metainfo(env, data, download_dir, paused)

	return 'either "filename" or "metainfo" must be included', {}


def _added_stub(entity: Entity) -> Dict[str, Any]:
	return {
		"id": entity.get_component(TorrentEC).index,
		"hashString": entity.get_component(TorrentEC).info_hash.hex(),
		"name": get_torrent_name(entity),
	}


async def _add_metainfo(env: Env, data: bytes, download_dir: Optional[Path], paused: bool):
	try:
		file_info = Metainfo(decode(data))
		info_hash = file_info.make_info_hash()
	except Exception as ex:  # noqa: BLE001 - any parse failure is a client error
		return f"invalid or corrupt torrent metainfo: {ex}", {}

	existing = get_torrent_entity(env, info_hash)
	if existing is not None:
		return "success", {"torrent-duplicate": _added_stub(existing)}

	await asyncio.gather(*env.event_bus.dispatch("request.metainfo.add", file_info, download_dir))

	torrent_entity = get_torrent_entity(env, info_hash)
	if not torrent_entity:
		return "Add torrent operation failed", {}

	if paused:
		env.event_bus.dispatch("request.torrent.stop", info_hash)

	logger.info("torrent-add: added %s", info_hash.hex())
	return "success", {"torrent-added": _added_stub(torrent_entity)}


async def _add_magnet(env: Env, magnet_link: str, paused):
	magnet = MagnetInfo(magnet_link)
	if not magnet.is_valid():
		return "invalid magnet link", {}

	info_hash = magnet.info_hash
	existing = get_torrent_entity(env, info_hash)
	if existing is not None:
		return "success", {"torrent-duplicate": _added_stub(existing)}

	await asyncio.gather(*env.event_bus.dispatch("request.magnet.add", magnet_link))

	torrent_entity = get_torrent_entity(env, info_hash)
	if not torrent_entity:
		return "add magnet operation failed", {}

	if paused:
		env.event_bus.dispatch("request.torrent.stop", info_hash)

	logger.info("torrent-add: queued magnet %s, paused %s", info_hash.hex(), paused)

	return "success", {"torrent-added": _added_stub(torrent_entity)}


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
async def session_stats(env, info, arguments):
	entities = list(env.data_storage.get_collection(TorrentEC))
	active = sum(1 for e in entities if is_torrent_active(e))
	downloaded = sum(e.get_component(TorrentStatsEC).downloaded for e in entities)
	uploaded = sum(e.get_component(TorrentStatsEC).uploaded for e in entities)
	seconds_active = int(time.monotonic() - info.start_time)

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
def _free_space(path: Path) -> int:
	try:
		return shutil.disk_usage(path).free
	except OSError:
		return -1


@method("free-space")
async def free_space(env, info, arguments):
	path = arguments.get("path", "")
	try:
		usage = shutil.disk_usage(Path(path) if path else env.config.download_folder)
	except OSError as ex:
		return f"free-space failed: {ex}", {}
	return "success", {"path": path, "size-bytes": usage.free, "total_size": usage.total}


@method("port-test")
async def port_test(env, info, arguments):
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
