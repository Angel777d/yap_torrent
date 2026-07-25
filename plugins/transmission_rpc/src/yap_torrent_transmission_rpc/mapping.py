"""Translation between the yap_torrent ECS model and Transmission RPC fields.

Transmission clients reference torrents by a small integer ``id`` that is only
stable within a session, or by the 40-char SHA1 ``hashString``.  yap_torrent
identifies torrents solely by ``info_hash``, so :class:`IdManager` hands out
session-scoped integer ids keyed by info_hash hex, and everything else is
derived from the torrent entity's components on demand.
"""
import logging
from typing import Any, Callable, Dict, List, Optional

from angelovich.core.DataStorage import Entity

from yap_torrent.components.peer_ec import PeerConnectionEC
from yap_torrent.components.torrent_ec import (
	TorrentEC,
	TorrentInfoEC,
	TorrentPathEC,
	TorrentState,
	TorrentStatsEC,
	ValidateTorrentEC,
)
from yap_torrent.components.tracker_ec import TorrentTrackerEC
from yap_torrent.env import Env
from yap_torrent.protocol import TorrentInfo
from yap_torrent.systems import get_torrent_name, is_torrent_complete, iterate_peers

logger = logging.getLogger(__name__)

# Transmission torrent status codes (see rpc-spec.md section 3.3).
TR_STATUS_STOPPED = 0
TR_STATUS_CHECK_WAIT = 1
TR_STATUS_CHECK = 2
TR_STATUS_DOWNLOAD_WAIT = 3
TR_STATUS_DOWNLOAD = 4
TR_STATUS_SEED_WAIT = 5
TR_STATUS_SEED = 6

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


class IdManager:
	def __init__(self):
		self._by_hash: Dict[str, int] = {}
		self._next_id = 1

	def id_for_hash(self, info_hash_hex: str) -> int:
		if info_hash_hex not in self._by_hash:
			self._by_hash[info_hash_hex] = self._next_id
			self._next_id += 1
		return self._by_hash[info_hash_hex]

	def hash_for_id(self, torrent_id: int) -> Optional[str]:
		for info_hash_hex, assigned in self._by_hash.items():
			if assigned == torrent_id:
				return info_hash_hex
		return None


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


def _files(entity: Entity, info: Optional[TorrentInfo], percent: float) -> List[Dict[str, Any]]:
	if not info:
		return []
	# TODO: per-file completion is approximated from the overall percentage.
	#  The ECS tracks completion per piece, not per file; map piece ranges to
	#  files for exact bytesCompleted.
	complete = is_torrent_complete(entity)
	result: List[Dict[str, Any]] = []
	for file in info.files:
		name = "/".join(part.decode("utf-8") for part in file.path)
		done = file.length if complete else int(file.length * percent)
		result.append({"name": name, "length": file.length, "bytesCompleted": done})
	return result


def _file_stats(entity: Entity, info: Optional[TorrentInfo], percent: float) -> List[Dict[str, Any]]:
	return [
		{"bytesCompleted": f["bytesCompleted"], "wanted": True, "priority": 0}
		for f in _files(entity, info, percent)
	]


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


def build_torrent(entity: Entity, fields, ids: IdManager, env: Env) -> Dict[str, Any]:
	"""Build a single torrent object containing only the requested ``fields``."""
	torrent_ec = entity.get_component(TorrentEC)
	info_hash_hex = torrent_ec.info_hash.hex()
	stats = entity.get_component(TorrentStatsEC)
	info = _torrent_info(entity)

	size = info.size if info else 0
	percent = info.calculate_downloaded(torrent_ec.bitfield.have_num) if info else 0.0
	downloaded = stats.downloaded
	uploaded = stats.uploaded
	peers = list(iterate_peers(env, torrent_ec.info_hash))
	path_ec = entity.get_component(TorrentPathEC)

	# Every getter is lazy so we only compute what the client asked for.
	getters: Dict[str, Callable[[], Any]] = {
		"id": lambda: ids.id_for_hash(info_hash_hex),
		"hashString": lambda: info_hash_hex,
		"name": lambda: get_torrent_name(entity),
		"status": lambda: status_code(entity),
		"percentDone": lambda: percent,
		"percentComplete": lambda: percent,
		"recheckProgress": lambda: 0.0,
		"metadataPercentComplete": lambda: 1.0 if info else 0.0,
		"totalSize": lambda: size,
		"sizeWhenDone": lambda: size,
		"leftUntilDone": lambda: int(size * (1 - percent)),
		"haveValid": lambda: int(size * percent),
		"haveUnchecked": lambda: 0,
		"desiredAvailable": lambda: int(size * (1 - percent)),
		"downloadedEver": lambda: downloaded,
		"uploadedEver": lambda: uploaded,
		"corruptEver": lambda: 0,
		"uploadRatio": lambda: (uploaded / downloaded) if downloaded else 0.0,
		"rateDownload": lambda: 0,  # TODO: core does not expose per-tick transfer rates.
		"rateUpload": lambda: 0,  # TODO: core does not expose per-tick transfer rates.
		"eta": lambda: -1,  # TODO: needs a download-rate estimate to compute.
		"peersConnected": lambda: len(peers),
		"peersGettingFromUs": lambda: sum(
			1 for p in peers if not p.get_component(PeerConnectionEC).remote_choked
		),
		"peersSendingToUs": lambda: sum(
			1 for p in peers if not p.get_component(PeerConnectionEC).local_choked
		),
		"webseedsSendingToUs": lambda: 0,
		"error": lambda: 0,  # TODO: surface tracker/announce failures as tr_stat errors.
		"errorString": lambda: "",
		"isFinished": lambda: bool(info and is_torrent_complete(entity)),
		"isStalled": lambda: False,
		"isPrivate": lambda: False,
		"downloadDir": lambda: path_ec.root_path.as_posix() if path_ec.root_path else "",
		"pieceCount": lambda: info.pieces_num if info else 0,
		"pieceSize": lambda: info.piece_length if info else 0,
		"addedDate": lambda: 0,  # TODO: core does not persist an added timestamp.
		"doneDate": lambda: 0,  # TODO
		"startDate": lambda: 0,  # TODO
		"activityDate": lambda: 0,  # TODO
		"editDate": lambda: 0,
		"dateCreated": lambda: 0,
		"seedRatioLimit": lambda: 0.0,
		"seedRatioMode": lambda: 0,
		"seedIdleLimit": lambda: 0,
		"seedIdleMode": lambda: 0,
		"queuePosition": lambda: ids.id_for_hash(info_hash_hex) - 1,
		"labels": lambda: [],
		"group": lambda: "",
		"downloadLimit": lambda: 0,
		"downloadLimited": lambda: False,
		"uploadLimit": lambda: 0,
		"uploadLimited": lambda: False,
		"honorsSessionLimits": lambda: True,
		"bandwidthPriority": lambda: 0,
		"files": lambda: _files(entity, info, percent),
		"fileStats": lambda: _file_stats(entity, info, percent),
		"priorities": lambda: [0] * (len(tuple(info.files)) if info else 0),
		"wanted": lambda: [1] * (len(tuple(info.files)) if info else 0),
		"trackers": lambda: _trackers(entity),
		"trackerStats": lambda: [],  # TODO: expose per-tracker announce statistics.
		"peers": lambda: [],  # TODO: expose the connected-peer detail list.
		"comment": lambda: "",
		"creator": lambda: "",
		"magnetLink": lambda: f"magnet:?xt=urn:btih:{info_hash_hex}",
	}

	result: Dict[str, Any] = {}
	for field in fields:
		getter = getters.get(field)
		if getter is not None:
			result[field] = getter()
		# Unsupported fields are omitted; Transmission clients tolerate absent keys.
	return result
