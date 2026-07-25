import logging
from pathlib import Path
from typing import Optional

from yap_torrent.components.torrent_ec import TorrentEC, SaveTorrentEC, ValidateTorrentEC
from yap_torrent.components.tracker_ec import TorrentTrackerEC, TorrentTrackerDataEC
from yap_torrent.protocol import Metainfo
from yap_torrent.system import System
from yap_torrent.systems import create_torrent_entity

logger = logging.getLogger(__name__)


class MetainfoSystem(System):
	async def start(self):
		self.env.event_bus.add_listener("request.metainfo.add", self._on_metainfo, scope=self)

	async def stop(self) -> None:
		self.env.event_bus.remove_all_listeners(scope=self)

	async def _on_metainfo(self, metainfo: Metainfo, path: Optional[Path] = None):
		info_hash = metainfo.make_info_hash()
		if self.env.data_storage.get_collection(TorrentEC).find(info_hash):
			logger.info(f"Torrent from {metainfo.info.name} is already exist")
			return

		# create a new torrent entity
		if not path:
			path = Path(self.env.config.download_folder)
		torrent_entity = create_torrent_entity(self.env, info_hash, path, {}, metainfo.info)

		# add tracker info
		announce_list = metainfo.announce_list
		if announce_list:
			torrent_entity.add_component(TorrentTrackerEC(announce_list))
			torrent_entity.add_component(TorrentTrackerDataEC())

		# save torrent to local data
		torrent_entity.add_component(SaveTorrentEC())

		# mark for files validation in case there are already downloaded files
		torrent_entity.add_component(ValidateTorrentEC())

		logger.info(f"New torrent added: {metainfo.info.name}")
