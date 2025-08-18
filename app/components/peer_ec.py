from asyncio import Future, Task
from typing import Hashable, Optional, Tuple

from core.DataStorage import EntityComponent
from torrent.connection import Connection
from torrent.structures import PeerInfo


class PeerInfoEC(EntityComponent):
	def __init__(self, info_hash: bytes, peer_info: PeerInfo, attempt: int = 0):
		super().__init__()
		self.info_hash: bytes = info_hash
		self.peer_info: PeerInfo = peer_info
		self.attempt = 0

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
	def __init__(self, connection: Connection, task: Task) -> None:
		super().__init__()

		self.connection: Connection = connection
		self.task: Task = task

		self.local_choked = True
		self.local_interested = False

		self.remote_choked = True
		self.remote_interested = False

		self.download_block: Optional[Tuple[int, int, int]] = None

	def _reset(self):
		self.connection.close()
		self.task.cancel()

		self.connection = None
		self.task = None

		super()._reset()

	async def interested(self) -> None:
		if self.local_interested:
			return
		await self.connection.interested()
		self.local_interested = True

	async def not_interested(self) -> None:
		if not self.local_interested:
			return
		await self.connection.not_interested()
		self.local_interested = False

	async def choke(self) -> None:
		if self.remote_choked:
			return
		await self.connection.choke()
		self.remote_choked = True

	async def unchoke(self) -> None:
		if not self.remote_choked:
			return
		await self.connection.unchoke()
		self.remote_choked = False

	async def request(self, index: int, begin: int, length: int) -> None:
		self.download_block = index, begin, length
		await self.connection.request(index, begin, length)
