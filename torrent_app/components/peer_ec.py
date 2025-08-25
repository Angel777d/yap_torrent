import logging
from asyncio import Task
from typing import Hashable, Set, Tuple

from core.DataStorage import EntityComponent
from torrent_app.protocol.connection import Connection
from torrent_app.protocol.structures import PeerInfo


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
	def __init__(self, connection: Connection, task: Task, queue_size: int = 10) -> None:
		super().__init__()

		self.connection: Connection = connection
		self.task: Task = task

		self.local_choked = True
		self.local_interested = False

		self.remote_choked = True
		self.remote_interested = False

		self.__in_progress: Set[Tuple[int, int]] = set()
		# TODO: move queue size to config
		self.__queue_size: int = queue_size

	def _reset(self):
		self.connection.close()
		self.task.cancel()

		self.connection = None
		self.task = None

		super()._reset()

	async def interested(self) -> None:
		if self.local_interested:
			return
		logging.debug(f"Interested in peer {self.connection.remote_peer_id}")
		await self.connection.interested()
		self.local_interested = True

	async def not_interested(self) -> None:
		if not self.local_interested:
			return
		logging.debug(f"Not interested in peer {self.connection.remote_peer_id}")
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
		self.__in_progress.add((index, begin))
		await self.connection.request(index, begin, length)

	def get_blocks(self, index: int) -> Set[int]:
		return set(block[1] for block in self.__in_progress if block[0] == index)

	def can_request(self) -> bool:
		return len(self.__in_progress) < self.__queue_size

	def reset_block(self, index, begin) -> None:
		try:
			self.__in_progress.remove((index, begin))
		except KeyError as ex:
			print(ex)

	def is_free_to_download(self) -> bool:
		return not self.__in_progress
