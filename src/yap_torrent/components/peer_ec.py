import logging
import time
from asyncio import Task
from enum import IntEnum
from typing import Hashable

from angelovich.core.DataStorage import EntityComponent, EntityHashComponent

from yap_torrent.protocol import bt_main_messages as msg
from yap_torrent.protocol.connection import Connection
from yap_torrent.protocol.structures import PeerInfo, PieceBlockInfo, Bitfield

logger = logging.getLogger(__name__)


class PeerState(IntEnum):
	Unknown = 0
	Questionable = 1
	NoConnection = 2
	Suspicious = 3
	Good = 4


class PeerEC(EntityHashComponent):
	def __init__(self, info_hash: bytes, peer_info: PeerInfo, state: PeerState = PeerState.Unknown) -> None:
		super().__init__()
		self.info_hash: bytes = info_hash
		self.peer_info: PeerInfo = peer_info

		self.state: PeerState = state
		self.fail_count: int = 0
		self.last_attempt: float = 0.0
		self.remote_bitfield: Bitfield = Bitfield()

	@staticmethod
	def make_hash(info_hash: bytes, host: str, port: int) -> Hashable:
		return info_hash, host, port

	def key(self) -> Hashable:
		"""Stable identity, unlike id(), which entity reuse invalidates."""
		return self.make_hash(self.info_hash, self.peer_info.host, self.peer_info.port)

	def __hash__(self):
		return hash(self.key())


class LocalInterestedEC(EntityComponent):
	"""We are interested in this peer (it has pieces in our wanted set)."""
	pass


class RemoteUnchokedEC(EntityComponent):
	"""The remote peer has unchoked us (we may request)."""
	pass


class RemoteInterestedEC(EntityComponent):
	"""The peer is interested in our pieces."""
	pass


class LocalUnchokedEC(EntityComponent):
	"""We have unchoked this peer (we serve it)."""
	pass


class PeerStatsEC(EntityComponent):
	def __init__(self) -> None:
		super().__init__()
		self.uploaded: int = 0
		self.downloaded: int = 0

	def add_uploaded(self, n: int) -> None:
		self.uploaded += n

	def add_downloaded(self, n: int) -> None:
		self.downloaded += n


class PeerRateEC(EntityComponent):
	def __init__(self) -> None:
		super().__init__()
		self.up_rate: float = 0.0
		self.down_rate: float = 0.0
		self._up_window: int = 0
		self._down_window: int = 0
		self._last_sample: float = time.monotonic()

	def add_uploaded(self, n: int) -> None:
		self._up_window += n

	def add_downloaded(self, n: int) -> None:
		self._down_window += n

	def sample_rate(self, now: float) -> None:
		dt = now - self._last_sample
		if dt <= 0:
			return
		self.up_rate = self._up_window / dt
		self.down_rate = self._down_window / dt
		self._up_window = 0
		self._down_window = 0
		self._last_sample = now


class PeerConnectionEC(EntityComponent):
	def __init__(self, info_hash: bytes, peer_info: PeerInfo, connection: Connection, reserved: bytes) -> None:
		super().__init__()

		self.peer_info: PeerInfo = peer_info
		self.info_hash: bytes = info_hash

		self.connection: Connection = connection

		self.task: Task = None

		self.reserved: bytes = reserved

		self.requested: set = set()

	def disconnect(self):
		if self.task:
			self.task.cancel()
		self.connection.close()

	def _reset(self):
		if self.task:
			self.task.cancel()
		self.connection.close()
		super()._reset()

	async def request(self, block: PieceBlockInfo) -> None:
		await self.connection.send(msg.request(block.index, block.begin, block.length))

	async def send(self, message: bytes) -> None:
		await self.connection.send(message)

	@property
	def connection_time(self) -> float:
		return self.connection.connection_time

	def __repr__(self):
		return f"Peer {self.peer_info.host} [{self.connection.remote_peer_id}]"


class PeerConnectionInProgressEC(EntityComponent):
	"""Marker: an outbound connection attempt is in flight (avoids double-connect)."""
	pass


class PeerDisconnectedEC(EntityComponent):
	pass

class PeerPendingRemoveEC(EntityComponent):
	pass
