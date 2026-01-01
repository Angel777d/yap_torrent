import time
from enum import Enum

from yap_torrent.dht.utils import compact_address


class DHTNodeState(Enum):
	UNKNOWN = 0
	GOOD = 1
	QUESTIONABLE = 2
	BAD = 3


class DHTNode:
	def __init__(self, node_id: bytes, host: str, port: int) -> None:
		self.id: bytes = node_id
		self.host: str = host
		self.port: int = port

		self.__last_update: float = time.monotonic()
		self.__state: DHTNodeState = DHTNodeState.UNKNOWN

	def mark_good(self):
		self.__state = DHTNodeState.GOOD
		self.__last_update = time.monotonic()
		return self

	def mark_fail(self):
		if self.__state == DHTNodeState.GOOD:
			self.__state = DHTNodeState.QUESTIONABLE
		else:
			self.__state = DHTNodeState.BAD
		return self

	def get_state(self) -> DHTNodeState:
		if self.__state == DHTNodeState.GOOD:
			if time.monotonic() - self.__last_update < 15 * 60:
				return DHTNodeState.GOOD
			else:
				return DHTNodeState.QUESTIONABLE
		return self.__state

	@property
	def compact_node_info(self) -> bytes:
		return self.id + compact_address(self.host, self.port)

	def __repr__(self):
		return f"DHT Node [{self.id}] {self.host}:{self.port}"
