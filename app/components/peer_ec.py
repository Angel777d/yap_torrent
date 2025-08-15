import asyncio
from typing import Hashable, Optional, Tuple

from core.DataStorage import EntityComponent
from torrent.connection import Connection
from torrent.structures import PeerInfo


class PeerInfoEC(EntityComponent):
	def __init__(self, info_hash: bytes, peer_info: PeerInfo):
		super().__init__()
		self.info_hash: bytes = info_hash
		self.peer_info: PeerInfo = peer_info

	@classmethod
	def is_hashable(cls) -> bool:
		return True

	@staticmethod
	def make_hash(info_hash: bytes, peer_info: PeerInfo) -> Hashable:
		return f"{peer_info}_{info_hash}"

	def get_hash(self) -> Hashable:
		return self.make_hash(self.info_hash, self.peer_info)


class PeerPendingEC(EntityComponent):
	pass


class PeerConnectionEC(EntityComponent):
	def __init__(self, connection: Connection) -> None:
		super().__init__()

		self.connection: Connection = connection

		self.local_choked = True
		self.local_interested = False

		self.remote_choked = True
		self.remote_interested = False

		self.download_block: Optional[Tuple[int, int, int]] = None

	def _reset(self):
		self.connection.close()
		super()._reset()

	def interested(self) -> None:
		if self.local_interested:
			return
		self.connection.interested()
		self.local_interested = True

	def not_interested(self) -> None:
		if not self.local_interested:
			return
		self.connection.not_interested()
		self.local_interested = False

	def choke(self) -> None:
		if self.remote_choked:
			return
		self.connection.interested()
		self.remote_choked = True

	def unchoke(self) -> None:
		if not self.remote_choked:
			return
		self.connection.not_interested()
		self.remote_choked = False

	def request(self, index: int, begin: int, length: int) -> None:
		self.download_block = index, begin, length
		self.connection.request(index, begin, length)


class PeerHandshakeEC(EntityComponent):
	def __init__(self, handshake_task: asyncio.Task) -> None:
		super().__init__()
		self.handshake_task: asyncio.Task = handshake_task

	def _reset(self):
		self.handshake_task.cancel()
		self.handshake_task = None
		super()._reset()


class PeerActiveEC(EntityComponent):
	def __init__(self, listen_task: asyncio.Task) -> None:
		super().__init__()
		self.listen_task: asyncio.Task = listen_task

	def _reset(self):
		self.listen_task.cancel()
		self.listen_task = None
		super()._reset()
