from pathlib import Path
from typing import Optional, Dict, Generator

from angelovich.core.DataStorage import Entity

from yap_torrent.components.peer_ec import KnownPeersEC, PeerConnectionEC
from yap_torrent.components.torrent_ec import TorrentInfoEC, TorrentEC, TorrentPathEC, TorrentStatsEC, \
	ValidateTorrentEC, TorrentState
from yap_torrent.env import Env
from yap_torrent.protocol import TorrentInfo


def is_torrent_complete(torrent_entity: Entity) -> bool:
	info = torrent_entity.get_component(TorrentInfoEC).info
	bitfield = torrent_entity.get_component(TorrentEC).bitfield
	return info.is_complete(bitfield.have_num)


def is_torrent_active(torrent_entity: Entity) -> bool:
	return (torrent_entity.is_valid()
	        and not (torrent_entity.has_component(ValidateTorrentEC)
	                 or torrent_entity.get_component(TorrentStatsEC).state == TorrentState.Inactive))


def calculate_downloaded(torrent_entity: Entity) -> float:
	info = torrent_entity.get_component(TorrentInfoEC).info
	bitfield = torrent_entity.get_component(TorrentEC).bitfield
	return info.calculate_downloaded(bitfield.have_num)


def create_torrent_entity(env: Env, info_hash: bytes, path: Optional[Path], stats: Dict[str, int],
                          torrent_info: Optional[TorrentInfo] = None, ) -> Entity:
	torrent_entity = env.data_storage.create_entity()
	torrent_entity.add_component(TorrentPathEC(path))
	torrent_entity.add_component(TorrentStatsEC(**stats))
	torrent_entity.add_component(KnownPeersEC())

	if torrent_info:
		torrent_entity.add_component(TorrentInfoEC(torrent_info))
	torrent_entity.add_component(TorrentEC(info_hash))
	return torrent_entity


def get_torrent_entity(env: Env, info_hash: bytes) -> Optional[Entity]:
	return env.data_storage.get_collection(TorrentEC).find(info_hash)


def get_info_hash(torrent_entity: Entity) -> bytes:
	return torrent_entity.get_component(TorrentEC).info_hash


def get_torrent_name(entity: Entity):
	if entity.has_component(TorrentInfoEC):
		return entity.get_component(TorrentInfoEC).info.name
	else:
		return f"[{entity.get_component(TorrentEC).info_hash}]"


def iterate_peers(env: Env, info_hash: bytes) -> Generator[Entity]:
	for e in env.data_storage.get_collection(PeerConnectionEC):
		if e.get_component(PeerConnectionEC).info_hash == info_hash:
			yield e
