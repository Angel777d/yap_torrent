import logging

from torrent_app import System
from torrent_app.components.torrent_ec import SaveTorrentEC
from torrent_app.components.tracker_ec import TorrentTrackerDataEC
from torrent_app.protocol.magnet import MagnetInfo
from torrent_app.systems import create_torrent_entity

logger = logging.getLogger(__name__)


class MagnetSystem(System):
	async def start(self) -> 'System':
		self.env.event_bus.add_listener("magnet.add", self.__on_magnet_add, scope=self)
		return await super().start()

	def close(self) -> None:
		super().close()
		self.env.event_bus.remove_all_listeners(scope=self)

	async def __on_magnet_add(self, value: str) -> None:
		magnet = MagnetInfo(value)

		if not magnet.is_valid():
			logger.info(f"magnet: {value} is invalid")
			return

		torrent_entity = create_torrent_entity(self.env, magnet.info_hash)
		if magnet.trackers:
			torrent_entity.add_component(TorrentTrackerDataEC([magnet.trackers]))
		# save torrent to local data
		torrent_entity.add_component(SaveTorrentEC())

		logger.info(f"add torrent by magnet: {magnet}")
