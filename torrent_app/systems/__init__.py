import asyncio
import concurrent.futures
from pathlib import Path
from typing import Optional, Callable, TypeVar, TypeVarTuple

from angelovichcore.DataStorage import Entity
from torrent_app import Env
from torrent_app.components.bitfield_ec import BitfieldEC
from torrent_app.components.peer_ec import KnownPeersEC, PeerConnectionEC, PeerInfoEC, PeerDisconnectedEC
from torrent_app.components.torrent_ec import TorrentInfoEC, TorrentHashEC, TorrentPathEC, ValidateTorrentEC
from torrent_app.protocol import TorrentInfo

_pool = concurrent.futures.ProcessPoolExecutor()


def invalidate_torrent(env: Env, torrent_entity: Entity):
	disconnect_all_peers_for(env, torrent_entity.get_component(TorrentHashEC).info_hash)
	torrent_entity.add_component(ValidateTorrentEC())


def disconnect_all_peers_for(env: Env, info_hash: bytes):
	all_connected_peers = env.data_storage.get_collection(PeerConnectionEC).entities
	for peer_entity in all_connected_peers:
		# find peers for this torrent
		if peer_entity.get_component(PeerInfoEC).info_hash == info_hash:
			peer_entity.get_component(PeerConnectionEC).disconnect()
			peer_entity.add_component(PeerDisconnectedEC())


def create_torrent_entity(
		env: Env,
		info_hash: bytes,
		torrent_info: Optional[TorrentInfo] = None,
		path: Optional[Path] = None
) -> Entity:
	if path is None:
		path = Path(env.config.download_folder)

	torrent_entity = env.data_storage.create_entity()
	torrent_entity.add_component(TorrentPathEC(path))
	torrent_entity.add_component(KnownPeersEC())
	torrent_entity.add_component(BitfieldEC())
	if torrent_info:
		torrent_entity.add_component(TorrentInfoEC(torrent_info))
	torrent_entity.add_component(TorrentHashEC(info_hash))
	return torrent_entity


_T = TypeVar("_T")
_Ts = TypeVarTuple("_Ts")


async def execute_in_pool(func: Callable[[*_Ts], _T], *args: *_Ts) -> _T:
	loop = asyncio.get_running_loop()
	return await loop.run_in_executor(_pool, func, *args)
