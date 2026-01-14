import logging
from asyncio import Task
from typing import Set, Iterable

from angelovich.core.DataStorage import EntityComponent, EntityHashComponent

from yap_torrent.protocol import bt_main_messages as msg
from yap_torrent.protocol.connection import Connection
from yap_torrent.protocol.structures import PeerInfo, PieceBlockInfo, Bitfield

logger = logging.getLogger(__name__)


class PeerInfoEC(EntityHashComponent):
	def __init__(self, info_hash: bytes, peer_info: PeerInfo):
		super().__init__()
		self.info_hash: bytes = info_hash
		self.peer_info: PeerInfo = peer_info

	def __hash__(self):
		return hash((self.info_hash, self.peer_info))


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

		self.remote_bitfield: Bitfield = Bitfield()

	def disconnect(self):
		self.task.cancel()
		self.connection.close()

	def _reset(self):
		self.task.cancel()
		self.connection.close()

		super()._reset()

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
