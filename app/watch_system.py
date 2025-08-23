import asyncio
import logging
import math
import os
import pickle
from pathlib import Path
from shutil import move

from app import System, Env
from app.components.bitfield_ec import BitfieldEC
from app.components.torrent_ec import TorrentInfoEC, TorrentSaveEC
from app.components.tracker_ec import TorrentTrackerDataEC, TorrentTrackerUpdatedEC
from app.utils import check_hash
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
		self.download_path = Path(env.config.download_folder)

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

		# move file to trash folder
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
				files_list.append((file_path, file_name))
				if file_path.suffix != ".torrent":
					continue

				torrent_info = load_torrent_file(file_path)
				if not torrent_info.is_valid():
					logger.info(f"Torrent file {file_path} is invalid")
					continue

				# TODO: check already added

				asyncio.create_task(self._check_torrent(torrent_info))

		return files_list

	async def _check_torrent(self, torrent_info: TorrentInfo):
		piece_length = torrent_info.pieces.piece_length
		bitfield = BitfieldEC(torrent_info.pieces.num)

		buffer: bytearray = bytearray()

		for file in torrent_info.files:
			path = torrent_info.get_file_path(self.download_path, file)
			if not path.exists():
				print(f"File {path} skipped at check")
				buffer.clear()
				continue

			with open(path, "rb") as f:
				bytes_left = file.length
				if not buffer:
					index = math.ceil(file.start / piece_length)
					current_piece_length = torrent_info.calculate_piece_size(index)
					offset = index * piece_length - file.start
					f.seek(offset)
					bytes_left -= offset
				while bytes_left > 0:
					bytes_to_read = min(bytes_left, current_piece_length)
					buffer.extend(f.read(bytes_to_read))
					bytes_left -= bytes_to_read
					current_piece_length -= bytes_to_read

					if current_piece_length > 0:
						continue

					if check_hash(bytes(buffer), torrent_info.pieces.get_piece_hash(index)):
						print(f"index {index} is ok")
						bitfield.set_index(index)
					else:
						print(f"index {index} is invalid")
					buffer.clear()
					index += 1
					current_piece_length = torrent_info.calculate_piece_size(index)

		logger.info(f"New torrent {torrent_info.name} added")
		entity = self.env.data_storage.create_entity()
		entity.add_component(TorrentInfoEC(torrent_info))
		entity.add_component(bitfield)
		entity.add_component(TorrentTrackerDataEC(torrent_info))
		entity.add_component(TorrentSaveEC())

	async def _load_local(self):
		for root, dirs, files in os.walk(self.active_path):
			for file_name in files:
				file_path = Path(root).joinpath(file_name)
				with open(file_path, 'rb') as f:
					torrent_info, bitfield, tracker_data = pickle.load(f)
					logger.info(f"{torrent_info.name} loaded from save")

					entity = self.env.data_storage.create_entity()
					entity.add_component(TorrentInfoEC(torrent_info))
					entity.add_component(BitfieldEC(torrent_info.pieces.num).update(bitfield))
					entity.add_component(TorrentTrackerDataEC(torrent_info).load(tracker_data))
					entity.add_component(TorrentTrackerUpdatedEC())

	async def _save_local(self, entity: Entity):
		torrent_info = entity.get_component(TorrentInfoEC).info
		bitfield = entity.get_component(BitfieldEC).dump()

		tracker_data = entity.get_component(TorrentTrackerDataEC).save()

		path = self.active_path.joinpath(str(torrent_info.name))

		def save():
			with open(path, 'wb') as f:
				pickle.dump((torrent_info, bitfield, tracker_data), f, pickle.HIGHEST_PROTOCOL)

		loop = asyncio.get_running_loop()
		await loop.run_in_executor(None, save)
