import logging
from pathlib import Path

from yap_torrent.components.torrent_ec import SaveTorrentEC
from yap_torrent.components.tracker_ec import TorrentTrackerDataEC, TorrentTrackerEC
from yap_torrent.protocol.magnet import MagnetInfo
from yap_torrent.system import System
from yap_torrent.systems import create_torrent_entity

logger = logging.getLogger(__name__)


class MagnetSystem(System):
	async def start(self):
		self.add_listener("request.magnet.add", self.__on_magnet_add)

	async def __on_magnet_add(self, value: str) -> None:
		magnet = MagnetInfo(value)

		if not magnet.is_valid():
			logger.info(f"magnet: {value} is invalid")
			return

		path = Path(self.env.config.download_folder)
		# the magnet's "dn" is all anyone has to go on until BEP-9 delivers the metadata,
		# which can be minutes away or never
		torrent_entity = create_torrent_entity(self.env, magnet.info_hash, path, {}, display_name=magnet.name)
		if magnet.trackers:
			torrent_entity.add_component(TorrentTrackerEC([magnet.trackers]))
			torrent_entity.add_component(TorrentTrackerDataEC())
		# save torrent to local data
		torrent_entity.add_component(SaveTorrentEC())

		logger.info(f"add torrent by magnet: {magnet}")
