import asyncio
import concurrent.futures
from pathlib import Path
from typing import Optional, Callable, TypeVar, TypeVarTuple, Dict

from angelovichcore.DataStorage import Entity
from torrent_app import Env
from torrent_app.components.bitfield_ec import BitfieldEC
from torrent_app.components.peer_ec import KnownPeersEC
from torrent_app.components.torrent_ec import TorrentInfoEC, TorrentHashEC, TorrentPathEC, TorrentStatsEC
from torrent_app.protocol import TorrentInfo

_pool = concurrent.futures.ProcessPoolExecutor()


def is_torrent_complete(torrent_entity: Entity) -> bool:
	info = torrent_entity.get_component(TorrentInfoEC).info
	bitfield = torrent_entity.get_component(BitfieldEC)
	return info.is_complete(bitfield.have_num)


def calculate_downloaded(torrent_entity: Entity) -> float:
	info = torrent_entity.get_component(TorrentInfoEC).info
	bitfield = torrent_entity.get_component(BitfieldEC)
	return info.calculate_downloaded(bitfield.have_num)


def create_torrent_entity(
		env: Env,
		info_hash: bytes,
		path: Optional[Path],
		stats: Dict[str, int],
		torrent_info: Optional[TorrentInfo] = None,
) -> Entity:
	torrent_entity = env.data_storage.create_entity()
	torrent_entity.add_component(TorrentPathEC(path))
	torrent_entity.add_component(TorrentStatsEC(**stats))
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


def get_torrent_name(entity: Entity):
	if entity.has_component(TorrentInfoEC):
		return entity.get_component(TorrentInfoEC).info.name
	else:
		return f"[{entity.get_component(TorrentHashEC).info_hash}]"
