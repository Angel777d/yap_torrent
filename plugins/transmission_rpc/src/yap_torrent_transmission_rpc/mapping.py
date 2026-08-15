"""Translation between the yap_torrent ECS model and Transmission RPC fields.

Transmission clients reference torrents by a small integer ``id`` that is only
stable within a session, or by the 40-char SHA1 ``hashString``.  yap_torrent
identifies torrents solely by ``info_hash``, so :class:`IdManager` hands out
session-scoped integer ids keyed by info_hash hex, and everything else is
derived from the torrent entity's components on demand.
"""
import base64
import logging
import mimetypes
import time
from dataclasses import dataclass
from typing import Any, Callable, Dict, List, Optional

from angelovich.core.DataStorage import Entity

from yap_torrent.components.file_ec import TorrentFileEC, TorrentFileProgressEC, TorrentFileStateEC
from yap_torrent.components.peer_ec import (
	LocalUnchokedEC,
	RemoteInterestedEC,
	RemoteUnchokedEC,
)
from yap_torrent.components.torrent_ec import (
	TorrentEC,
	TorrentInfoEC,
	TorrentLimitsEC,
	TorrentPathEC,
	TorrentQueuePositionEC,
	TorrentRateEC,
	TorrentState,
	TorrentStatsEC,
	ValidateTorrentEC,
)
from yap_torrent.components.tracker_ec import TorrentTrackerDataEC, TorrentTrackerEC
from yap_torrent.env import Env
from yap_torrent.protocol import TorrentInfo
from yap_torrent.systems import (
	get_local_id,
	get_torrent_name,
	is_torrent_complete,
	iterate_connected_peers,
	iterate_files,
)
from .components import get_labels
from .server_info import ServerInfo

logger = logging.getLogger(__name__)

# Transmission torrent status codes (see rpc-spec.md section 3.3).
TR_STATUS_STOPPED = 0
TR_STATUS_CHECK_WAIT = 1
TR_STATUS_CHECK = 2
TR_STATUS_DOWNLOAD_WAIT = 3
TR_STATUS_DOWNLOAD = 4
TR_STATUS_SEED_WAIT = 5
TR_STATUS_SEED = 6

# tr_stat_errtype (libtransmission/transmission.h)
TR_STAT_OK = 0
TR_STAT_TRACKER_WARNING = 1
TR_STAT_TRACKER_ERROR = 2
TR_STAT_LOCAL_ERROR = 3

# how long a running torrent may be idle before clients call it stalled; matches the
# queue-stalled-minutes value session-get reports
STALLED_AFTER_SECONDS = 30 * 60

# Fields returned by torrent-get when the client does not specify a "fields" list.
DEFAULT_FIELDS = (
	"id",
	"hashString",
	"name",
	"status",
	"percentDone",
	"totalSize",
	"downloadedEver",
	"uploadedEver",
	"rateDownload",
	"rateUpload",
	"peersConnected",
	"eta",
	"error",
	"errorString",
	"downloadDir",
)


def _torrent_info(entity: Entity) -> Optional[TorrentInfo]:
	if entity.has_component(TorrentInfoEC):
		return entity.get_component(TorrentInfoEC).info
	return None


def status_code(entity: Entity) -> int:
	if entity.has_component(ValidateTorrentEC):
		return TR_STATUS_CHECK
	if entity.get_component(TorrentStatsEC).state == TorrentState.Inactive:
		return TR_STATUS_STOPPED
	if is_torrent_complete(entity):
		return TR_STATUS_SEED
	return TR_STATUS_DOWNLOAD


def file_bytes_completed(file_entity: Entity) -> int:
	return file_entity.get_component(TorrentFileProgressEC).bytes_completed


def _file_entities(entity: Entity, env: Env) -> List[Entity]:
	"""The torrent's file entities, in file-index order."""
	files = list(iterate_files(env, entity.get_component(TorrentEC).info_hash))
	files.sort(key=lambda e: e.get_component(TorrentFileEC).index)
	return files


def _files(entity: Entity, info: Optional[TorrentInfo], env: Env) -> List[Dict[str, Any]]:
	if not info:
		return []
	result: List[Dict[str, Any]] = []
	for file_entity in _file_entities(entity, env):
		file_ec = file_entity.get_component(TorrentFileEC)
		result.append({
			"name": file_ec.path,
			"length": file_ec.length,
			"bytesCompleted": file_bytes_completed(file_entity),
		})
	return result


def _file_stats(entity: Entity, info: Optional[TorrentInfo], env: Env) -> List[Dict[str, Any]]:
	if not info:
		return []
	result: List[Dict[str, Any]] = []
	for file_entity in _file_entities(entity, env):
		state = file_entity.get_component(TorrentFileStateEC)
		result.append({
			"bytesCompleted": file_bytes_completed(file_entity),
			"wanted": state.is_wanted,
			"priority": int(state.priority),
		})
	return result


def _rates(entity: Entity) -> tuple:
	if entity.has_component(TorrentRateEC):
		rate = entity.get_component(TorrentRateEC)
		return int(rate.down_rate), int(rate.up_rate)
	return 0, 0


def _eta(left: int, rate_download: int, complete: bool) -> int:
	"""Seconds to completion. Transmission's -1 means unknown, -2 means not applicable."""
	if complete:
		return -2
	if rate_download <= 0:
		return -1
	return int(left / rate_download)


@dataclass(frozen=True)
class _Progress:
	"""Totals over the whole torrent and over the files the user actually wants.

	tr_stat splits these deliberately — percentComplete is "how much has been downloaded
	of the entire torrent", percentDone is "how much has been downloaded of the files the
	user wants", and sizeWhenDone/leftUntilDone are wanted-relative too. Reporting the
	total for all of them leaves a torrent with a deselected file stuck below 100% for
	ever while it reports itself as seeding.
	"""
	total_size: int
	wanted_size: int
	have_total: int
	have_wanted: int

	@property
	def percent_complete(self) -> float:
		return (self.have_total / self.total_size) if self.total_size else 0.0

	@property
	def percent_done(self) -> float:
		return (self.have_wanted / self.wanted_size) if self.wanted_size else 0.0

	@property
	def left_until_done(self) -> int:
		return max(0, self.wanted_size - self.have_wanted)


def _progress(entity: Entity, info: Optional[TorrentInfo], env: Env) -> _Progress:
	if not info:
		return _Progress(0, 0, 0, 0)

	files = _file_entities(entity, env)
	if not files:
		# metadata is in, but the file entities have not materialised yet: fall back to
		# whole-piece counting, where wanted and total are the same thing
		have = int(info.size * info.calculate_downloaded(entity.get_component(TorrentEC).bitfield.have_num))
		return _Progress(info.size, info.size, have, have)

	total_size = wanted_size = have_total = have_wanted = 0
	for file_entity in files:
		length = file_entity.get_component(TorrentFileEC).length
		done = file_bytes_completed(file_entity)
		total_size += length
		have_total += done
		if file_entity.get_component(TorrentFileStateEC).is_wanted:
			wanted_size += length
			have_wanted += done
	return _Progress(total_size, wanted_size, have_total, have_wanted)


def _queued_count(env: Env) -> int:
	"""How many torrents hold a queue ordinal — i.e. the first position after the queue."""
	return sum(1 for _ in env.data_storage.get_collection(TorrentQueuePositionEC))


def _limits(entity: Entity) -> TorrentLimitsEC:
	"""Per-torrent limits, or the defaults for a torrent that has never had any set."""
	if entity.has_component(TorrentLimitsEC):
		return entity.get_component(TorrentLimitsEC)
	return _DEFAULT_LIMITS


_DEFAULT_LIMITS = TorrentLimitsEC()


def _upload_ratio(uploaded: int, downloaded: int) -> float:
	# TR_RATIO_NA = -1 (nothing either way), TR_RATIO_INF = -2 (uploaded without downloading)
	if downloaded > 0:
		return uploaded / downloaded
	return -2.0 if uploaded > 0 else -1.0


def _is_stalled(entity: Entity, rate_download: int, rate_upload: int) -> bool:
	stats = entity.get_component(TorrentStatsEC)
	if stats.state != TorrentState.Active or rate_download or rate_upload:
		return False
	if not stats.activity_date:
		return False
	return (time.time() - stats.activity_date) > STALLED_AFTER_SECONDS


def _primary_mime_type(info: Optional[TorrentInfo]) -> str:
	if not info:
		return ""
	largest = max(info.files, key=lambda f: f.length, default=None)
	if largest is None:
		return ""
	name = "/".join(part.decode("utf-8", "replace") for part in largest.path)
	return mimetypes.guess_type(name)[0] or "application/octet-stream"


def _pieces(entity: Entity, info: Optional[TorrentInfo]) -> str:
	"""The local bitfield, base64'd — the shape tr_stat's "pieces" field uses."""
	if not info:
		return ""
	return base64.b64encode(entity.get_component(TorrentEC).bitfield.dump(info.pieces_num)).decode("ascii")


def _trackers(entity: Entity) -> List[Dict[str, Any]]:
	if not entity.has_component(TorrentTrackerEC):
		return []
	result: List[Dict[str, Any]] = []
	tracker_id = 0
	for tier_index, tier in enumerate(entity.get_component(TorrentTrackerEC).announce_list):
		for announce in tier:
			result.append({"id": tracker_id, "announce": announce, "scrape": "", "tier": tier_index})
			tracker_id += 1
	return result


def _tracker_stats(entity: Entity) -> List[Dict[str, Any]]:
	"""Per-tracker announce state. Core keeps one announce record per torrent, not per
	tracker, so every tracker of a torrent reports that same state."""
	if not entity.has_component(TorrentTrackerDataEC):
		return []
	data = entity.get_component(TorrentTrackerDataEC)
	stats = []
	for tracker in _trackers(entity):
		stats.append({
			**tracker,
			"host": tracker["announce"],
			"lastAnnounceSucceeded": not data.failure_reason,
			"lastAnnounceResult": data.failure_reason or data.warning_message or "",
			"announceState": 1 if data.failure_reason else 0,
			"nextAnnounceTime": int(data.last_update_time + data.interval),
		})
	return stats


def _error_code(entity: Entity) -> int:
	# a tracker failure is TR_STAT_TRACKER_ERROR (2), not TR_STAT_LOCAL_ERROR (3) — core
	# surfaces no local errors of its own yet
	if entity.has_component(TorrentTrackerDataEC):
		data = entity.get_component(TorrentTrackerDataEC)
		if data.failure_reason:
			return TR_STAT_TRACKER_ERROR
		if data.warning_message:
			return TR_STAT_TRACKER_WARNING
	return TR_STAT_OK


def _error_string(entity: Entity) -> str:
	if entity.has_component(TorrentTrackerDataEC):
		data = entity.get_component(TorrentTrackerDataEC)
		return data.failure_reason or data.warning_message or ""
	return ""


def build_torrent(entity: Entity, fields, server_info: ServerInfo) -> Dict[str, Any]:
	"""Build a single torrent object containing only the requested ``fields``.

	`server_info` carries the env and the plugin's entry point name, which the fields we
	keep in the torrent's custom_data rather than in a core component are keyed by.
	"""
	env = server_info.env
	torrent_ec = entity.get_component(TorrentEC)
	info_hash_hex = torrent_ec.info_hash.hex()
	stats = entity.get_component(TorrentStatsEC)
	info = _torrent_info(entity)

	size = info.size if info else 0
	downloaded = stats.downloaded
	uploaded = stats.uploaded
	peers = list(iterate_connected_peers(env, torrent_ec.info_hash))
	path_ec = entity.get_component(TorrentPathEC)
	rate_download, rate_upload = _rates(entity)
	complete = bool(info and is_torrent_complete(entity))
	progress = _progress(entity, info, env)
	left = progress.left_until_done
	limits = _limits(entity)

	# Every getter is lazy so we only compute what the client asked for.
	getters: Dict[str, Callable[[], Any]] = {
		"id": lambda: get_local_id(entity),
		"hashString": lambda: info_hash_hex,
		"name": lambda: get_torrent_name(entity),
		"status": lambda: status_code(entity),
		# percentDone is of the *wanted* files, percentComplete of the whole torrent
		"percentDone": lambda: progress.percent_done,
		"percentComplete": lambda: progress.percent_complete,
		"recheckProgress": lambda: 0.0,
		"metadataPercentComplete": lambda: 1.0 if info else 0.0,
		"totalSize": lambda: size,
		"sizeWhenDone": lambda: progress.wanted_size,
		"leftUntilDone": lambda: left,
		"haveValid": lambda: progress.have_total,
		"haveUnchecked": lambda: 0,
		"desiredAvailable": lambda: left,
		"downloadedEver": lambda: downloaded,
		"uploadedEver": lambda: uploaded,
		"corruptEver": lambda: 0,
		"uploadRatio": lambda: _upload_ratio(uploaded, downloaded),
		"rateDownload": lambda: rate_download,
		"rateUpload": lambda: rate_upload,
		"eta": lambda: _eta(left, rate_download, complete),
		"peersConnected": lambda: len(peers),
		# the two queues: we serve a peer that is unchoked by us and interested, and we
		# take from one that has unchoked us
		"peersGettingFromUs": lambda: sum(
			1 for p in peers if p.has_component(LocalUnchokedEC) and p.has_component(RemoteInterestedEC)
		),
		"peersSendingToUs": lambda: sum(1 for p in peers if p.has_component(RemoteUnchokedEC)),
		"webseedsSendingToUs": lambda: 0,
		"error": lambda: _error_code(entity),
		"errorString": lambda: _error_string(entity),
		"isFinished": lambda: complete,
		"isStalled": lambda: _is_stalled(entity, rate_download, rate_upload),
		"isPrivate": lambda: False,
		"downloadDir": lambda: path_ec.root_path.as_posix() if path_ec.root_path else "",
		"pieceCount": lambda: info.pieces_num if info else 0,
		"pieceSize": lambda: info.piece_length if info else 0,
		"addedDate": lambda: int(stats.added_date),
		"doneDate": lambda: int(stats.done_date),
		"startDate": lambda: int(stats.started_date),
		"activityDate": lambda: int(stats.activity_date),
		"editDate": lambda: 0,
		"dateCreated": lambda: 0,
		# stored per torrent, enforced by nothing yet (see TorrentLimitsEC)
		"seedRatioLimit": lambda: limits.seed_ratio_limit,
		"seedRatioMode": lambda: limits.seed_ratio_mode,
		"seedIdleLimit": lambda: 0,
		"seedIdleMode": lambda: 0,
		# A torrent gets its ordinal when it gains metadata, so a magnet has none yet.
		# Core sorts those last (`math.inf` in iterate_torrents_in_queue_order); the JSON
		# equivalent is one past the last real position — reporting 0 put it level with the
		# torrent actually at the head of the queue, and -1 would sort it above that.
		"queuePosition": lambda: (
			entity.get_component(TorrentQueuePositionEC).position
			if entity.has_component(TorrentQueuePositionEC) else _queued_count(env)
		),
		"labels": lambda: get_labels(entity, server_info.name),
		"group": lambda: "",
		"downloadLimit": lambda: limits.download_limit,
		"downloadLimited": lambda: limits.download_limited,
		"uploadLimit": lambda: limits.upload_limit,
		"uploadLimited": lambda: limits.upload_limited,
		"honorsSessionLimits": lambda: limits.honors_session_limits,
		"bandwidthPriority": lambda: limits.bandwidth_priority,
		"peer-limit": lambda: limits.peer_limit or env.config.peer_limit_per_torrent,
		"maxConnectedPeers": lambda: env.config.peer_limit_per_torrent,
		"files": lambda: _files(entity, info, env),
		"fileStats": lambda: _file_stats(entity, info, env),
		"priorities": lambda: [f["priority"] for f in _file_stats(entity, info, env)],
		"wanted": lambda: [int(f["wanted"]) for f in _file_stats(entity, info, env)],
		"trackers": lambda: _trackers(entity),
		"trackerStats": lambda: _tracker_stats(entity),
		"peers": lambda: [],  # TODO: expose the connected-peer detail list.
		"comment": lambda: "",
		"creator": lambda: "",
		"magnetLink": lambda: f"magnet:?xt=urn:btih:{info_hash_hex}",
		# fields core does not track, answered with the spec's "unknown" values rather
		# than omitted: transmission-rpc reads them as self.fields[name] and raises
		# KeyError on anything absent
		"eta_idle": lambda: -1,
		"etaIdle": lambda: -1,
		"file-count": lambda: len(tuple(info.files)) if info else 0,
		"manualAnnounceTime": lambda: -1,
		"primary-mime-type": lambda: _primary_mime_type(info),
		"pieces": lambda: _pieces(entity, info),
		"sequentialDownload": lambda: False,
		"torrentFile": lambda: "",
		"trackerList": lambda: "\n".join(t["announce"] for t in _trackers(entity)),
		"webseeds": lambda: [],
		# TODO: secondsDownloading / secondsSeeding need per-torrent active-time
		#  accounting, and availability needs a per-piece count over peer bitfields.
		"secondsDownloading": lambda: 0,
		"secondsSeeding": lambda: 0,
		"availability": lambda: [],
	}

	result: Dict[str, Any] = {}
	for field in fields:
		getter = getters.get(field)
		if getter is not None:
			result[field] = getter()
	# Unsupported fields are omitted; Transmission clients tolerate absent keys.
	return result
