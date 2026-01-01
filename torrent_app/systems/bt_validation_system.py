import asyncio
import logging
import math
from asyncio import Task
from pathlib import Path
from typing import Set, Optional

from torrent_app import System, Env
from torrent_app.components.bitfield_ec import BitfieldEC
from torrent_app.components.torrent_ec import TorrentPathEC, ValidateTorrentEC, TorrentInfoEC, SaveTorrentEC
from torrent_app.protocol import TorrentInfo
from torrent_app.systems import execute_in_pool, calculate_downloaded
from torrent_app.utils import check_hash

logger = logging.getLogger(__name__)


class ValidationSystem(System):
	def __init__(self, env: Env):
		super().__init__(env)

		self._collection = self.env.data_storage.get_collection(ValidateTorrentEC)
		self._task: Optional[Task[Set[int]]] = None

	def close(self):
		super().close()
		if self._task:
			self._task.cancel()

	async def _update(self, delta_time: float):
		# some validation in process
		if self._task:
			return

		for torrent_entity in self._collection.entities:

			torrent_info = torrent_entity.get_component(TorrentInfoEC).info
			download_path = torrent_entity.get_component(TorrentPathEC).root_path

			def reset_task(_task: Task[Set[int]]):
				self._task = None
				if _task.cancelled():
					return

				torrent_entity.get_component(BitfieldEC).reset(_task.result())

				# save torrent to local data
				torrent_entity.add_component(SaveTorrentEC())

				# reset validate flag
				torrent_entity.remove_component(ValidateTorrentEC)

				logger.info(
					f"Validation complete: {torrent_info.name}. {calculate_downloaded(torrent_entity):.2%} downloaded")

			logger.info(f"Validation start: {torrent_info.name}")

			torrent_entity.get_component(BitfieldEC).reset({})

			task = asyncio.create_task(execute_in_pool(_check_torrent, torrent_info, download_path))
			task.add_done_callback(reset_task)
			self._task = task

			break


def _check_torrent(torrent_info: TorrentInfo, download_path: Path) -> Set[int]:
	piece_length: int = torrent_info.piece_length
	bitfield_data: Set[int] = set()

	buffer: bytearray = bytearray()
	for file in torrent_info.files:
		try:
			path = torrent_info.get_file_path(download_path, file)
			if not path.exists():
				buffer.clear()
				continue

			with open(path, "rb") as f:
				bytes_left = file.length
				if not buffer:
					index: int = math.ceil(file.start / piece_length)
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

					if check_hash(bytes(buffer), torrent_info.get_piece_hash(index)):
						bitfield_data.add(index)

					buffer.clear()
					index += 1
					current_piece_length = torrent_info.calculate_piece_size(index)
		except Exception as ex:
			logger.error(f"Error while validating torrent {download_path}: {ex}")

	return bitfield_data
