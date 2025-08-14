import math
from typing import Hashable, Dict

from core.DataStorage import EntityComponent


class PieceEC(EntityComponent):
	__BLOCK_SIZE = 2 ** 14  # (16kb)

	def __init__(self, info_hash: bytes, index: int, length: int):
		super().__init__()
		self.info_hash: bytes = info_hash
		self.index: int = index
		self.__length = length
		self.__data: Dict[int, bytes] = {}
		self.__begin = 0  # block_index * block_size

	def append(self, begin: int, block: bytes):
		block_index = begin // self.block_size
		self.__data[block_index] = block

	def get_next_begin(self):
		result = self.__begin
		self.__begin += self.block_size
		return result

	@property
	def completed(self):
		blocks_num = math.ceil(self.__length / self.block_size)
		return len(self.__data) == blocks_num

	@property
	def block_size(self):
		return self.__BLOCK_SIZE

	@classmethod
	def is_hashable(cls) -> bool:
		return True

	def get_hash(self) -> Hashable:
		return self.make_hash(self.info_hash, self.index)

	@staticmethod
	def make_hash(info_hash: bytes, index: int) -> Hashable:
		return info_hash, index


class CompletedPieceEC(EntityComponent):
	pass
