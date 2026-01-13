import asyncio
import logging

from yap_torrent.components.torrent_ec import TorrentState, TorrentStatsEC
from yap_torrent.system import System
from yap_torrent.systems import get_torrent_entity

logger = logging.getLogger(__name__)


class TorrentSystem(System):

	async def start(self):
		self.env.event_bus.add_listener("request.torrent.start", self._on_torrent_start, scope=self)
		self.env.event_bus.add_listener("request.torrent.stop", self._on_torrent_stop, scope=self)
		self.env.event_bus.add_listener("request.torrent.remove", self._on_torrent_remove, scope=self)

	def close(self) -> None:
		super().close()
		self.env.event_bus.remove_all_listeners(scope=self)

	async def _update(self, delta_time: float):
		pass

	async def _on_torrent_start(self, info_hash: bytes):
		torrent_entity = get_torrent_entity(self.env, info_hash)
		torrent_entity.get_component(TorrentStatsEC).state = TorrentState.Active
		await asyncio.gather(*self.env.event_bus.dispatch("action.torrent.start", info_hash))

	async def _on_torrent_stop(self, info_hash: bytes):
		logger.info(f"Stopping torrent {info_hash.hex()}")
		torrent_entity = get_torrent_entity(self.env, info_hash)
		torrent_entity.get_component(TorrentStatsEC).state = TorrentState.Inactive
		await asyncio.gather(*self.env.event_bus.dispatch("action.torrent.stop", info_hash))
		logger.info(f"Stopping torrent {info_hash.hex()} complete")

	async def _on_torrent_remove(self, info_hash: bytes):
		logger.info(f"Remove torrent {info_hash.hex()}")
		await self._on_torrent_stop(info_hash)
		await asyncio.gather(*self.env.event_bus.dispatch("action.torrent.remove", info_hash))
		self.env.data_storage.remove_entity(get_torrent_entity(self.env, info_hash))
		logger.info(f"Remove torrent {info_hash.hex()} complete")
