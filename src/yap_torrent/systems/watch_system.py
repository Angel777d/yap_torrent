import logging
import os
from pathlib import Path
from shutil import move

from yap_torrent.components.torrent_ec import ValidateTorrentEC, TorrentEC, SaveTorrentEC
from yap_torrent.components.tracker_ec import TorrentTrackerDataEC, TorrentTrackerEC
from yap_torrent.env import Env
from yap_torrent.protocol import load_torrent_file
from yap_torrent.system import System
from yap_torrent.systems import create_torrent_entity

logger = logging.getLogger(__name__)


class WatcherSystem(System):
	def __init__(self, env: Env):
		super().__init__(env)
		self.last_update = 0

		self.trash_path = Path(env.config.trash_folder)
		self.watch_path = Path(env.config.watch_folder)

	async def start(self):
		self.trash_path.mkdir(parents=True, exist_ok=True)
		self.watch_path.mkdir(parents=True, exist_ok=True)

	async def _update(self, delta_time: float):
		files_to_move = await self._load_from_path(self.watch_path)

		# move file to the trash folder
		for file_path, file_name in files_to_move:
			move(file_path, self.trash_path.joinpath(file_name))

	async def _load_from_path(self, path: Path):
		files_list = []
		for root, dirs, files in os.walk(path):
			for file_name in files:
				file_path = Path(root).joinpath(file_name)
				files_list.append((file_path, file_name))
				if file_path.suffix != ".torrent":
					continue

				torrent_file_data = load_torrent_file(file_path)
				if not torrent_file_data:
					logger.info(f"Torrent file {file_path} is invalid")
					continue

				info_hash = torrent_file_data.make_info_hash()
				if self.env.data_storage.get_collection(TorrentEC).find(info_hash):
					logger.info(f"Torrent from {file_path} is already exist")
					continue

				# create a new torrent entity
				path = Path(self.env.config.download_folder)
				torrent_entity = create_torrent_entity(self.env, info_hash, path, {}, torrent_file_data.info)

				# add tracker info
				announce_list = torrent_file_data.announce_list
				if announce_list:
					torrent_entity.add_component(TorrentTrackerEC(announce_list))
					torrent_entity.add_component(TorrentTrackerDataEC())

				# save torrent to local data
				torrent_entity.add_component(SaveTorrentEC())

				# mark for files validation in case there are already downloaded files
				torrent_entity.add_component(ValidateTorrentEC())

				logger.info(f"New torrent added from {file_path}")

		return files_list
