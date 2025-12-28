import logging
import time
from typing import Hashable, Set

from angelovichcore.DataStorage import EntityComponent
from torrent_app.protocol.structures import PieceInfo, PieceBlock
from torrent_app.utils import check_hash

logger = logging.getLogger(__name__)


class PieceEC(EntityComponent):
	__BLOCK_SIZE = 2 ** 14  # (16kb)

	def __init__(self, info_hash: bytes, info: PieceInfo):
		super().__init__()
		self.info_hash = info_hash
		self.info = info
		self.data: bytes = bytes()

	def set_data(self, data: bytes) -> bool:
		if check_hash(data, self.info.piece_hash):
			self.data = data
			return True

		return False

	@property
	def completed(self) -> bool:
		return len(self.data) > 0

	@classmethod
	def is_hashable(cls) -> bool:
		return True

	def get_hash(self) -> Hashable:
		return self.make_hash(self.info_hash, self.info.index)

	@staticmethod
	def make_hash(info_hash: bytes, index: int) -> Hashable:
		return info_hash, index

	def get_block(self, begin, length) -> bytes:
		return self.data[begin:begin + length]


class PieceBlocksEC(EntityComponent):
	_BLOCK_SIZE = 2 ** 14  # (16kb)

	def __init__(self, info: PieceInfo):
		super().__init__()
		self.info = info

		self._downloaded_size: int = 0
		self._downloaded: bytearray = bytearray(info.size)

		self._blocks: Set[int] = set()

	@staticmethod
	def _block_size():
		return PieceBlocksEC._BLOCK_SIZE

	@staticmethod
	def create_blocks(piece: PieceInfo) -> Set[PieceBlock]:
		begin = 0
		block_size = PieceBlocksEC._block_size()
		result: Set[PieceBlock] = set()
		while begin < piece.size:
			result.add(
				PieceBlock(piece.index, begin, PieceBlocksEC._calculate_block_size(piece.size, begin)))
			begin += block_size
		return result

	@staticmethod
	def _calculate_block_size(size: int, begin: int) -> int:
		block_size = PieceBlocksEC._block_size()
		if begin + block_size > size:
			return size % block_size
		return block_size

	def add_block(self, block: PieceBlock, data: bytes) -> bool:
		# basic validation of block
		if block.length != len(data):
			logger.warning(f"Invalid {block} length.")
			return False

		# TODO: endgame - skip already downloaded blocks
		if block.begin in self._blocks:
			logger.warning(f"Second block {block} length.")
			return False

		self._blocks.add(block.begin)

		self._downloaded[block.begin:block.begin + block.length] = data
		self._downloaded_size += block.length
		return True

	def is_full(self) -> bool:
		return self.info.size == self._downloaded_size

	# construct data from downloaded blocks
	def pull_data_and_reset(self) -> bytes:
		if not self.is_full():
			logger.error("Try to pull data from not full piece.")
			return bytes()

		self._downloaded_size = 0
		self._blocks.clear()

		return bytes(self._downloaded)


class PieceToSaveEC(EntityComponent):
	pass


class PiecePendingRemoveEC(EntityComponent):
	REMOVE_TIMEOUT = 15  # TODO: move to config

	def __init__(self) -> None:
		super().__init__()
		self.__last_update: float = 0

	def update(self):
		self.__last_update = time.monotonic()

	def can_remove(self) -> bool:
		return time.monotonic() - self.__last_update > self.REMOVE_TIMEOUT

	@property
	def last_update(self) -> float:
		return self.__last_update
