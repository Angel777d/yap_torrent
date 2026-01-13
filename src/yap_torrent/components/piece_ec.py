import logging
import time
from typing import Hashable

from angelovich.core.DataStorage import EntityComponent, EntityHashComponent

from yap_torrent.protocol.structures import PieceInfo
from yap_torrent.utils import check_hash

logger = logging.getLogger(__name__)


class PieceEC(EntityHashComponent):
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

	@staticmethod
	def make_hash(info_hash: bytes, index: int) -> Hashable:
		return info_hash, index

	def __hash__(self):
		return hash(self.make_hash(self.info_hash, self.info.index))

	def get_block(self, begin, length) -> bytes:
		return self.data[begin:begin + length]


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
