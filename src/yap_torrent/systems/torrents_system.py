import logging

from yap_torrent.components.torrent_ec import TorrentState, TorrentStatsEC
from yap_torrent.system import System
from yap_torrent.systems import get_torrent_entity

logger = logging.getLogger(__name__)


class TorrentSystem(System):

	async def start(self):
		self.add_listener("request.torrent.start", self._on_torrent_start)
		self.add_listener("request.torrent.stop", self._on_torrent_stop)
		self.add_listener("request.torrent.remove", self._on_torrent_remove)

	async def _update(self, delta_time: float):
		pass

	async def _on_torrent_start(self, info_hash: bytes):
		torrent_entity = get_torrent_entity(self.env, info_hash)
		if not torrent_entity:
			logger.warning(f"[TorrentSystem] _on_torrent_start: torrent {info_hash.hex()} not found")
			return
		torrent_entity.get_component(TorrentStatsEC).state = TorrentState.Active
		await self.env.event_bus.dispatch_async("action.torrent.start", torrent_entity)

	async def _on_torrent_stop(self, info_hash: bytes):
		logger.info(f"Stopping torrent {info_hash.hex()}")
		torrent_entity = get_torrent_entity(self.env, info_hash)
		if not torrent_entity:
			logger.warning(f"[TorrentSystem] _on_torrent_stop: torrent {info_hash.hex()} not found")
			return
		torrent_entity.get_component(TorrentStatsEC).state = TorrentState.Inactive
		await self.env.event_bus.dispatch_async("action.torrent.stop", torrent_entity)
		logger.info(f"Stopping torrent {info_hash.hex()} complete")

	async def _on_torrent_remove(self, info_hash: bytes, delete_data: bool = False):
		logger.info(f"Remove torrent {info_hash.hex()}")
		torrent_entity = get_torrent_entity(self.env, info_hash)
		if not torrent_entity:
			logger.warning(f"[TorrentSystem] can't remove torrent {info_hash.hex()}. Not found")
			return
		await self._on_torrent_stop(info_hash)
		await self.env.event_bus.dispatch_async("action.torrent.remove", info_hash)
		self.env.data_storage.remove_entity(get_torrent_entity(self.env, info_hash))
		logger.info(f"Remove torrent {info_hash.hex()} complete")
