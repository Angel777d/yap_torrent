import asyncio
import logging
import math
from asyncio import Task
from pathlib import Path
from typing import Set, Optional

from yap_torrent.components.torrent_ec import TorrentPathEC, ValidateTorrentEC, TorrentInfoEC, SaveTorrentEC, TorrentEC, \
	TorrentStatsEC, TorrentState
from yap_torrent.env import Env
from yap_torrent.protocol import TorrentInfo
from yap_torrent.system import System
from yap_torrent.systems import calculate_downloaded, get_torrent_entity
from yap_torrent.utils import check_hash, execute_in_pool

logger = logging.getLogger(__name__)


class ValidationSystem(System):
	def __init__(self, env: Env):
		super().__init__(env)

		self._collection = self.env.data_storage.get_collection(ValidateTorrentEC)
		self._task: Optional[Task[Set[int]]] = None

	async def start(self):
		self.env.event_bus.add_listener("request.torrent.invalidate", self._on_torrent_invalidate, scope=self)

	async def _on_torrent_invalidate(self, info_hash: bytes):
		torrent_entity = get_torrent_entity(self.env, info_hash)
		torrent_entity.add_component(ValidateTorrentEC())
		self.env.event_bus.dispatch("action.torrent.stop", info_hash)

	def close(self):
		self.env.event_bus.remove_all_listeners(scope=self)
		if self._task:
			self._task.cancel()
		super().close()

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

				torrent_entity.get_component(TorrentEC).bitfield.reset(_task.result())

				# save torrent to local data
				torrent_entity.add_component(SaveTorrentEC())

				# reset validate flag
				torrent_entity.remove_component(ValidateTorrentEC)

				# start torrent if needed
				if torrent_entity.get_component(TorrentStatsEC).state == TorrentState.Active:
					info_hash = torrent_entity.get_component(TorrentEC).info_hash
					self.env.event_bus.dispatch("action.torrent.start", info_hash)

				logger.info(
					f"Validation complete: {torrent_info.name}. {calculate_downloaded(torrent_entity):.2%} downloaded")

			logger.info(f"Validation start: {torrent_info.name}")

			torrent_entity.get_component(TorrentEC).bitfield.reset(set())

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
