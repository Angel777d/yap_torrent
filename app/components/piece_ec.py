import hashlib
import logging
import time
from typing import Hashable, List, Tuple

from core.DataStorage import EntityComponent
from torrent.structures import PieceInfo

logger = logging.getLogger(__name__)


class PieceEC(EntityComponent):
	__BLOCK_SIZE = 2 ** 14  # (16kb)

	def __init__(self, info_hash: bytes, piece_info: PieceInfo):
		super().__init__()
		self.data: bytearray = bytearray()

		self.info_hash: bytes = info_hash
		self.__piece_info: PieceInfo = piece_info
		self.__begin = 0  # block_index * block_size

		# last piece size calculations
		self.__size = piece_info.piece_length
		if (piece_info.index + 1) * piece_info.piece_length > piece_info.full_size:
			self.__size = piece_info.full_size % piece_info.piece_length
		self.__downloaded: int = 0

		self.__in_progress: List[Tuple[bytes, float, int, int, int]] = []
		self.__canceled: List[Tuple[bytes, float, int, int, int]] = []


	@property
	def index(self) -> int:
		return self.__piece_info.index

	def has_next(self) -> bool:
		return bool(self.__canceled or self.__begin < self.__size)

	def calculate_block_size(self, begin: int) -> int:
		if begin + self.__block_size > self.__size:
			return self.__size % self.__block_size
		return self.__block_size

	def get_next(self, peer_id: bytes) -> Tuple[int, int, int]:
		if self.__canceled:
			_, _, index, begin, block_size = self.__canceled.pop()
			self.__in_progress.append((peer_id, time.time(), self.index, begin, block_size))
			return index, begin, block_size

		begin = self.__begin
		block_size = self.calculate_block_size(begin)
		self.__begin += block_size
		self.__in_progress.append((peer_id, time.time(), self.index, begin, block_size))
		return self.index, begin, block_size

	def cancel(self, peer_id: bytes):
		self.__canceled.extend(p for p in self.__in_progress if p[0] == peer_id)
		self.__in_progress = [p for p in self.__in_progress if p[0] != peer_id]

	def append(self, begin: int, block: bytes):
		self.data[begin:begin + len(block)] = block
		self.__downloaded += len(block)

		# check piece is corrupted and reset piece
		if self.completed and not self.__piece_info.piece_hash == hashlib.sha1(self.data).digest():
			logger.warning(f"piece {self.index} is corrupted. reset")

			self.__downloaded = 0
			self.__begin = 0
			self.__canceled = []

	@property
	def completed(self):
		return self.__downloaded >= self.__size

	@property
	def __block_size(self):
		return self.__BLOCK_SIZE

	@classmethod
	def is_hashable(cls) -> bool:
		return True

	def get_hash(self) -> Hashable:
		return self.make_hash(self.info_hash, self.index)

	@staticmethod
	def make_hash(info_hash: bytes, index: int) -> Hashable:
		return info_hash, index


class PieceToSaveEC(EntityComponent):
	pass
