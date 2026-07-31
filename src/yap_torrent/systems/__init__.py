import math
from pathlib import Path
from typing import Any, Optional, Dict, Generator, List, Set

from angelovich.core.DataStorage import Entity

from yap_torrent.components.common import IdleEC
from yap_torrent.components.file_ec import TorrentFileEC, TorrentFileStateEC
from yap_torrent.components.peer_ec import PeerConnectionEC, PeerEC, PeerState, PeerStatsEC, PeerPendingRemoveEC
from yap_torrent.components.torrent_ec import TorrentInfoEC, TorrentEC, TorrentCustomDataEC, TorrentPathEC, \
	TorrentStatsEC, SaveTorrentEC, ValidateTorrentEC, TorrentState, TorrentDownloadProgressEC, TorrentQueuePositionEC
from yap_torrent.env import Env
from yap_torrent.protocol import InfoHash
from yap_torrent.protocol import TorrentInfo
from yap_torrent.protocol.structures import Bitfield
from yap_torrent.protocol.structures import PeerInfo


def interested_pieces(torrent_entity: Entity, remote_bitfield: Bitfield) -> Set[int]:
	missing = torrent_entity.get_component(TorrentEC).bitfield.interested_in(remote_bitfield)
	if torrent_entity.has_component(TorrentDownloadProgressEC):
		return missing.intersection(torrent_entity.get_component(TorrentDownloadProgressEC).wanted.have)
	return missing


def is_torrent_complete(torrent_entity: Entity) -> bool:
	if not torrent_entity.has_component(TorrentDownloadProgressEC):
		return False
	bitfield = torrent_entity.get_component(TorrentEC).bitfield
	wanted = torrent_entity.get_component(TorrentDownloadProgressEC).wanted
	return len(bitfield.interested_in(wanted)) == 0


def is_torrent_active(torrent_entity: Entity) -> bool:
	return (torrent_entity.is_valid()
	        and torrent_entity.get_component(TorrentStatsEC).state == TorrentState.Active
	        and not torrent_entity.has_component(ValidateTorrentEC)
	        )


def calculate_downloaded(torrent_entity: Entity) -> float:
	info = torrent_entity.get_component(TorrentInfoEC).info
	bitfield = torrent_entity.get_component(TorrentEC).bitfield
	return info.calculate_downloaded(bitfield.have_num)


def create_torrent_entity(env: Env, info_hash: InfoHash, path: Path, stats: Dict[str, int],
                          torrent_info: Optional[TorrentInfo] = None, ) -> Entity:
	torrent_entity = env.data_storage.create_entity()
	torrent_entity.add_component(TorrentPathEC(path))
	torrent_entity.add_component(TorrentStatsEC(**stats))

	if torrent_info:
		torrent_entity.add_component(TorrentInfoEC(torrent_info))
	torrent_entity.add_component(TorrentEC(info_hash))
	return torrent_entity


def get_torrent_entity(env: Env, info_hash: InfoHash) -> Optional[Entity]:
	return env.data_storage.get_collection(TorrentEC).find(info_hash)


def get_info_hash(torrent_entity: Entity) -> InfoHash:
	return torrent_entity.get_component(TorrentEC).info_hash


def get_torrent_name(entity: Entity):
	if entity.has_component(TorrentInfoEC):
		return entity.get_component(TorrentInfoEC).info.name
	else:
		return f"[{entity.get_component(TorrentEC).info_hash}]"


def iterate_connected_peers(env: Env, info_hash: bytes) -> Generator[Entity]:
	"""Iterate the *connected* peer entities of a torrent (have PeerConnectionEC)."""
	for e in env.data_storage.get_collection(PeerConnectionEC):
		if e.get_component(PeerEC).info_hash == info_hash:
			yield e


def iterate_peers(env: Env, info_hash: bytes) -> Generator[Entity]:
	"""Iterate all known peer entities of a torrent (have PeerEC), connected or not."""
	for e in env.data_storage.get_collection(PeerEC):
		if e.has_component(PeerPendingRemoveEC):
			continue
		if e.get_component(PeerEC).info_hash == info_hash:
			yield e


def iterate_torrents_in_queue_order(env: Env) -> List[Entity]:
	def position(entity: Entity):
		if entity.has_component(TorrentQueuePositionEC):
			return entity.get_component(TorrentQueuePositionEC).position
		return math.inf

	return sorted(env.data_storage.get_collection(TorrentEC), key=position)


def find_peer_entity(env: Env, info_hash: bytes, host: str, port: int) -> Optional[Entity]:
	return env.data_storage.get_collection(PeerEC).find(PeerEC.make_hash(info_hash, host, port))


def add_known_peer(env: Env, info_hash: bytes, peer_info: PeerInfo) -> Entity:
	"""Find-or-create the peer entity for (info_hash, host, port). New peers start Unknown."""
	entity = find_peer_entity(env, info_hash, peer_info.host, peer_info.port)
	if entity is None:
		entity = env.data_storage.create_entity()
		entity.add_component(PeerEC(info_hash, peer_info, PeerState.Unknown))
		entity.add_component(IdleEC())
		entity.add_component(PeerStatsEC())

	return entity


def compute_wanted_bitfield(env: Env, info_hash: InfoHash, info: TorrentInfo) -> Bitfield:
	wanted = Bitfield()
	files = list(iterate_files(env, info_hash))
	if not files:
		for index in range(info.pieces_num):
			wanted.set_index(index)
		return wanted

	for file_entity in files:
		if not file_entity.get_component(TorrentFileStateEC).wanted:
			continue
		file_ec = file_entity.get_component(TorrentFileEC)
		for index in range(file_ec.first_piece, file_ec.first_piece + file_ec.pieces_length):
			wanted.set_index(index)
	return wanted


def mark_for_save(torrent_entity: Entity) -> None:
	"""Ask LocalDataSystem to write this torrent out on its next tick."""
	if not torrent_entity.has_component(SaveTorrentEC):
		torrent_entity.add_component(SaveTorrentEC())


def get_custom_data(torrent_entity: Entity, plugin_name: str, default: Any = None) -> Any:
	"""Whatever `plugin_name` last stored on this torrent, or `default`."""
	if not torrent_entity.has_component(TorrentCustomDataEC):
		return default
	return torrent_entity.get_component(TorrentCustomDataEC).data.get(plugin_name, default)


def set_custom_data(torrent_entity: Entity, plugin_name: str, value: Any) -> None:
	"""Store `value` under `plugin_name` and mark the torrent for saving.

	Core does not read the value and has no opinion on its shape, so what goes in it — and
	whether writing the same thing twice is worth a save — is the plugin's call.
	"""
	if not torrent_entity.has_component(TorrentCustomDataEC):
		torrent_entity.add_component(TorrentCustomDataEC())
	torrent_entity.get_component(TorrentCustomDataEC).data[plugin_name] = value
	mark_for_save(torrent_entity)


def iterate_files(env: Env, info_hash: bytes) -> Generator[Entity]:
	for e in env.data_storage.get_collection(TorrentFileEC):
		if e.get_component(TorrentFileEC).info_hash == info_hash:
			yield e
