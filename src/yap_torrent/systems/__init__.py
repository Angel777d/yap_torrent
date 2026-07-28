from pathlib import Path
from typing import Optional, Dict, Generator, Iterator

from angelovich.core.DataStorage import Entity

from yap_torrent.components.common import IdleEC
from yap_torrent.components.file_ec import TorrentFileEC, TorrentFileStateEC
from yap_torrent.components.peer_ec import PeerConnectionEC, PeerEC, PeerState, PeerStatsEC, PeerPendingRemoveEC
from yap_torrent.components.torrent_ec import TorrentInfoEC, TorrentEC, TorrentPathEC, TorrentStatsEC, \
	ValidateTorrentEC, TorrentState, TorrentDownloadProgressEC
from yap_torrent.env import Env
from yap_torrent.protocol import InfoHash
from yap_torrent.protocol import TorrentInfo
from yap_torrent.protocol.structures import Bitfield
from yap_torrent.protocol.structures import PeerInfo


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

def iterate_active_torrents(env: Env) -> Generator[Entity]:
	return (e for e in env.data_storage.get_collection(TorrentEC) if is_torrent_active(e))


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


def iterate_files(env: Env, info_hash: bytes) -> Generator[Entity]:
	for e in env.data_storage.get_collection(TorrentFileEC):
		if e.get_component(TorrentFileEC).info_hash == info_hash:
			yield e
