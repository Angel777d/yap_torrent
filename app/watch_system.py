import asyncio
import logging
import os
import pickle
from pathlib import Path
from shutil import move
from typing import Optional

from app import System, Env
from app.components.bitfield_ec import BitfieldEC
from app.components.torrent_ec import TorrentInfoEC, TorrentSaveEC
from app.components.tracker_ec import TorrentTrackerDataEC, TorrentTrackerUpdatedEC
from core.DataStorage import Entity
from torrent import load_torrent_file, TorrentInfo

logger = logging.getLogger(__name__)


class WatcherSystem(System):
	def __init__(self, env: Env):
		super().__init__(env)
		self.last_update = 0

		self.trash_path = Path(env.config.trash_folder)
		self.watch_path = Path(env.config.watch_folder)
		self.active_path = Path(env.config.active_folder)

	async def __check_folders(self):
		self.trash_path.mkdir(parents=True, exist_ok=True)
		self.watch_path.mkdir(parents=True, exist_ok=True)
		self.active_path.mkdir(parents=True, exist_ok=True)

	async def start(self) -> System:
		await self.__check_folders()
		await self._load_local()
		return self

	async def _update(self, delta_time: float):
		files_to_move = await self._load_from_path(self.watch_path)

		# move file to active folder
		for file_path, file_name in files_to_move:
			move(file_path, self.trash_path.joinpath(file_name))

		# save local torrent data
		to_save = self.env.data_storage.get_collection(TorrentSaveEC).entities
		for entity in to_save:
			entity.remove_component(TorrentSaveEC)
			await self._save_local(entity)

	async def _load_from_path(self, path: Path):
		files_list = []
		for root, dirs, files in os.walk(path):
			for file_name in files:
				file_path = Path(root).joinpath(file_name)
				if file_path.suffix != ".torrent":
					continue

				torrent_info = load_torrent_file(file_path)
				if not torrent_info.is_valid():
					logger.info(f"Torrent file {file_path} is invalid")
					continue

				logger.info(f"New torrent file {file_path} added")
				entity = await self._add_torrent(torrent_info)
				entity.add_component(TorrentSaveEC())

				files_list.append((file_path, file_name))
		return files_list

	async def _load_local(self):
		for root, dirs, files in os.walk(self.active_path):
			for file_name in files:
				file_path = Path(root).joinpath(file_name)
				with open(file_path, 'rb') as f:
					info, bitfield, tracker_data = pickle.load(f)
					logger.info(f"{info.name} loaded from save")
					await self._add_torrent(info, bitfield, tracker_data)

	async def _save_local(self, entity: Entity):
		info = entity.get_component(TorrentInfoEC).info
		bitfield = entity.get_component(BitfieldEC).dump()

		# TODO: serialize/deserialize
		tracker_data = entity.get_component(TorrentTrackerDataEC)
		tracker_data = (
			tracker_data.min_interval,
			tracker_data.interval,
			tracker_data.last_update_time,
			tracker_data.peers
		)

		path = self.active_path.joinpath(str(info.name))

		def save():
			with open(path, 'wb') as f:
				pickle.dump((info, bitfield, tracker_data), f, pickle.HIGHEST_PROTOCOL)

		loop = asyncio.get_running_loop()
		await loop.run_in_executor(None, save)

	async def _add_torrent(self, torrent_info: TorrentInfo, bitfield: bytes = bytes(),
	                       tracker_data: Optional[TorrentTrackerDataEC] = None) -> Entity:
		entity = self.env.data_storage.create_entity()
		entity.add_component(TorrentInfoEC(torrent_info))
		entity.add_component(BitfieldEC(torrent_info.pieces.num).update(bitfield))
		entity.add_component(TorrentTrackerDataEC(torrent_info.info_hash, torrent_info.announce_list))

		if tracker_data:
			min_interval, interval, t, peers = tracker_data

			component = entity.get_component(TorrentTrackerDataEC)
			component.min_interval = min_interval
			component.interval = interval
			component.last_update_time = t
			component.peers = peers

			entity.add_component(TorrentTrackerUpdatedEC())

		return entity
