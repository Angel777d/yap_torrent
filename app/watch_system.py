import os
import pickle
from pathlib import Path
from shutil import move

from app import System, Env
from app.components.bitfield_ec import BitfieldEC
from app.components.torrent_ec import TorrentInfoEC
from app.components.tracker_ec import TorrentTrackerDataEC
from core.DataStorage import Entity
from torrent import load_torrent_file, TorrentInfo


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

	async def update(self, delta_time: float):
		files_to_move = await self._load_from_path(self.watch_path)

		# move file to active folder
		for file_path, file_name in files_to_move:
			move(file_path, self.trash_path.joinpath(file_name))

	async def _load_from_path(self, path: Path):
		files_list = []
		for root, dirs, files in os.walk(path):
			for file_name in files:
				file_path = Path(root).joinpath(file_name)
				if file_path.suffix != ".torrent":
					continue

				torrent_info = load_torrent_file(file_path)
				if not torrent_info.is_valid():
					print("can't read torrent file:", file_path)
					continue

				print("new torrent added from path:", file_path)
				entity = await self._add_torrent(torrent_info, BitfieldEC.create_empty(torrent_info))
				await self._save_local(entity)

				files_list.append((file_path, file_name))
		return files_list

	async def _load_local(self):
		for root, dirs, files in os.walk(self.active_path):
			for file_name in files:
				file_path = Path(root).joinpath(file_name)
				with open(file_path, 'rb') as f:
					info, bitfield = pickle.load(f)
					print(f"{info.name} loaded from locals")
					await self._add_torrent(info, bitfield)

	async def _save_local(self, entity: Entity):
		info = entity.get_component(TorrentInfoEC).info
		bitfield = entity.get_component(BitfieldEC)._bitfield
		path = self.active_path.joinpath(str(info.info_hash))
		with open(path, 'wb') as f:
			pickle.dump((info, bitfield), f, pickle.HIGHEST_PROTOCOL)

	async def _add_torrent(self, torrent_info: TorrentInfo, bitfield: bytearray) -> Entity:
		entity = self.env.data_storage.create_entity()
		entity.add_component(TorrentInfoEC(torrent_info))
		entity.add_component(BitfieldEC(bitfield))
		entity.add_component(TorrentTrackerDataEC(torrent_info.info_hash, torrent_info.announce_list))

		return entity
