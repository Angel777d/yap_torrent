import logging
from asyncio import Task
from typing import Hashable, Set, Iterable

from angelovichcore.DataStorage import EntityComponent
from torrent_app.protocol import bt_main_messages as msg
from torrent_app.protocol.connection import Connection
from torrent_app.protocol.structures import PeerInfo, PieceBlockInfo

logger = logging.getLogger(__name__)


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
		return peer_info, info_hash

	def get_hash(self) -> Hashable:
		return self.make_hash(self.info_hash, self.peer_info)


class PeerConnectionEC(EntityComponent):
	def __init__(self, connection: Connection, reserved: bytes, queue_size: int = 15) -> None:
		super().__init__()

		self.connection: Connection = connection

		self.task: Task = None

		self.reserved: bytes = reserved

		self.local_choked = True
		self.local_interested = False

		self.remote_choked = True
		self.remote_interested = False

	def disconnect(self):
		self.task.cancel()
		self.connection.close()

	def _reset(self):
		self.task.cancel()
		self.connection.close()

		super()._reset()

	async def interested(self) -> None:
		if self.local_interested:
			return
		logger.debug(f"Interested in peer {self.connection.remote_peer_id}")
		await self.connection.send(msg.interested())
		self.local_interested = True

	async def not_interested(self) -> None:
		if not self.local_interested:
			return
		logger.debug(f"Not interested in peer {self.connection.remote_peer_id}")
		await self.connection.send(msg.not_interested())
		self.local_interested = False

	async def choke(self) -> None:
		if self.remote_choked:
			return
		await self.connection.send(msg.choke())
		self.remote_choked = True

	async def unchoke(self) -> None:
		if not self.remote_choked:
			return
		await self.connection.send(msg.unchoke())
		self.remote_choked = False

	async def request(self, block: PieceBlockInfo) -> None:
		await self.connection.send(msg.request(block.index, block.begin, block.length))

	def __repr__(self):
		return f"Peer [{self.connection.remote_peer_id}]"


class KnownPeersEC(EntityComponent):
	def __init__(self):
		super().__init__()
		self.peers: Set[PeerInfo] = set()

	def update_peers(self, peers: Iterable[PeerInfo]) -> "KnownPeersEC":
		logger.debug(f"New peers: {set(peers) - self.peers}")
		self.peers.update(peers)
		return self


class PeerDisconnectedEC(EntityComponent):
	pass
