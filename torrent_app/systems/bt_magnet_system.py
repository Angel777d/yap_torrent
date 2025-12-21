import logging

from torrent_app import System
from torrent_app.components.bitfield_ec import BitfieldEC
from torrent_app.components.peer_ec import KnownPeersEC
from torrent_app.components.torrent_ec import TorrentHashEC
from torrent_app.components.tracker_ec import TorrentTrackerDataEC
from torrent_app.protocol.magnet import MagnetInfo

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

		entity = self.env.data_storage.create_entity()
		entity.add_component(KnownPeersEC())
		entity.add_component(BitfieldEC())
		entity.add_component(TorrentHashEC(magnet.info_hash))

		if magnet.trackers:
			entity.add_component(TorrentTrackerDataEC([magnet.trackers]))

		logger.info(f"add torrent by magnet: {magnet}")
