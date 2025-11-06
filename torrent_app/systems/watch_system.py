import asyncio
import logging
import math
import os
import pickle
from pathlib import Path
from shutil import move

from angelovichcore.DataStorage import Entity
from torrent_app import System, Env
from torrent_app.components.bitfield_ec import BitfieldEC
from torrent_app.components.peer_ec import KnownPeersEC, KnownPeersUpdateEC
from torrent_app.components.torrent_ec import TorrentInfoEC, TorrentSaveEC, TorrentHashEC
from torrent_app.components.tracker_ec import TorrentTrackerDataEC
from torrent_app.protocol import load_torrent_file
from torrent_app.protocol.structures import TorrentFileInfo
from torrent_app.utils import check_hash

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

	async def start(self):
		await self.__check_folders()
		await self._load_local()

	async def _update(self, delta_time: float):
		files_to_move = await self._load_from_path(self.watch_path)

		# move file to trash folder
		for file_path, file_name in files_to_move:
			move(file_path, self.trash_path.joinpath(file_name))

		# save local protocol data
		to_save = self.env.data_storage.get_collection(TorrentSaveEC).entities
		for entity in to_save:
			entity.remove_component(TorrentSaveEC)
			await self._save_local(entity)

	def close(self):
		to_save = self.env.data_storage.get_collection(TorrentInfoEC).entities
		for entity in to_save:
			self.save(entity)
		super().close()

	def save(self, entity: Entity):
		info_hash = entity.get_component(TorrentHashEC).info_hash
		torrent_info = entity.get_component(TorrentInfoEC).info
		bitfield = entity.get_component(BitfieldEC).dump(torrent_info.pieces.num)
		peers = entity.get_component(KnownPeersEC).peers

		tracker_data = None
		if entity.has_component(TorrentTrackerDataEC):
			tracker_data = entity.get_component(TorrentTrackerDataEC).export_save()

		path = self.active_path.joinpath(str(torrent_info.name))
		with open(path, 'wb') as f:
			logger.debug(f"Save torrent data {torrent_info.name}")
			pickle.dump((info_hash, torrent_info, bitfield, peers, tracker_data), f, pickle.HIGHEST_PROTOCOL)

	async def _load_from_path(self, path: Path):
		files_list = []
		for root, dirs, files in os.walk(path):
			for file_name in files:
				file_path = Path(root).joinpath(file_name)
				files_list.append((file_path, file_name))
				if file_path.suffix != ".torrent":
					continue

				torrent_info = load_torrent_file(file_path)
				if not torrent_info:
					logger.info(f"Torrent file {file_path} is invalid")
					continue

				# TODO: check already added

				asyncio.create_task(self._check_torrent(torrent_info))

		return files_list

	async def _check_torrent(self, file_info: TorrentFileInfo):
		torrent_info = file_info.info
		piece_length = torrent_info.pieces.piece_length
		bitfield = BitfieldEC()

		buffer: bytearray = bytearray()

		for file in torrent_info.files:
			path = torrent_info.get_file_path(self.download_path, file)
			if not path.exists():
				logger.debug(f"File {path} does not exist. skipping")
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
						logger.debug(f"piece {index} is YES")
						bitfield.set_index(index)
					else:
						logger.debug(f"piece {index} is NO")

					buffer.clear()
					index += 1
					current_piece_length = torrent_info.calculate_piece_size(index)

		downloaded = bitfield.have_num * torrent_info.pieces.piece_length
		downloaded = min(downloaded, torrent_info.size) / torrent_info.size * 100

		logger.info(f"New torrent {torrent_info.name} added. Local data: {downloaded:.2f}%")
		entity = self.env.data_storage.create_entity()
		entity.add_component(TorrentHashEC(file_info.info_hash))
		entity.add_component(KnownPeersEC())
		entity.add_component(bitfield)

		entity.add_component(TorrentInfoEC(torrent_info))

		entity.add_component(TorrentTrackerDataEC(file_info.announce_list))
		entity.add_component(TorrentSaveEC())

	async def _load_local(self):
		for root, dirs, files in os.walk(self.active_path):
			for file_name in files:
				file_path = Path(root).joinpath(file_name)
				with open(file_path, 'rb') as f:
					logger.debug(f"loading save from {file_path}")
					info_hash, torrent_info, bitfield, peers, tracker_data = pickle.load(f)
					logger.info(f"{torrent_info.name} loaded from save")

					entity = self.env.data_storage.create_entity()
					entity.add_component(TorrentHashEC(info_hash))
					entity.add_component(KnownPeersEC().update_peers(peers))
					entity.add_component(BitfieldEC().update(bitfield))

					entity.add_component(TorrentInfoEC(torrent_info))

					entity.add_component(KnownPeersUpdateEC())

					# update tracker data if any
					if tracker_data:
						entity.add_component(TorrentTrackerDataEC(tracker_data.announce_list).import_save(tracker_data))

	async def _save_local(self, entity: Entity):
		loop = asyncio.get_running_loop()
		await loop.run_in_executor(None, self.save, entity)
