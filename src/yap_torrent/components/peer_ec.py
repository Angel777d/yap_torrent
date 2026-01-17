import logging
import time
from asyncio import Task
from typing import Set, Iterable, Iterator, Dict

from angelovich.core.DataStorage import EntityComponent

from yap_torrent.protocol import bt_main_messages as msg
from yap_torrent.protocol.connection import Connection
from yap_torrent.protocol.structures import PeerInfo, PieceBlockInfo, Bitfield

logger = logging.getLogger(__name__)


class PeerConnectionEC(EntityComponent):
	def __init__(self, info_hash: bytes, peer_info: PeerInfo, connection: Connection, reserved: bytes) -> None:
		super().__init__()

		self.peer_info: PeerInfo = peer_info
		self.info_hash: bytes = info_hash

		self.connection: Connection = connection

		self.task: Task = None

		self.reserved: bytes = reserved

		self.local_choked = True
		self.local_interested = False

		self.remote_choked = True
		self.remote_interested = False

		self.remote_bitfield: Bitfield = Bitfield()

	def __hash__(self):
		return hash(self.peer_info.host)

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
		return f"Peer {self.peer_info.host} [{self.connection.remote_peer_id}]"


class KnownPeersEC(EntityComponent):
	_MAX_CONNECT_ATTEMPTS = 5
	_COOLDOWN_DURATION = 30

	def __init__(self):
		super().__init__()
		self._peers: Set[PeerInfo] = set()
		self._fails: Dict[str, int] = {}
		self._last_attempts: Dict[str, float] = {}

	@property
	def peers(self) -> Set[PeerInfo]:
		return set(p for p in self._peers if self._fails[p.host] < self._MAX_CONNECT_ATTEMPTS)

	def update_peers(self, peers: Iterable[PeerInfo]):
		new_peers = set(peers) - self._peers
		logger.debug("New peers amount: %s", len(new_peers))
		self._peers.update(new_peers)

		for peer in new_peers:
			self._fails[peer.host] = 0
			self._last_attempts[peer.host] = 0

	def get_fails_count(self, peer: PeerInfo):
		return self._fails.get(peer.host, 0)

	def mark_good(self, peer: PeerInfo):
		self._fails[peer.host] = 0

	def mark_failed(self, peer: PeerInfo):
		self._fails[peer.host] += 1
		self._last_attempts[peer.host] = time.monotonic()

	def get_peers_to_connect(self, active_peers: Set[str]) -> Iterator[PeerInfo]:
		for peer in self._peers:
			if peer.host in active_peers:
				continue

			# give up after 5 failed attempts
			if self._fails[peer.host] > self._MAX_CONNECT_ATTEMPTS:
				continue

			# on a cooldown, skip
			if time.monotonic() - self._last_attempts[peer.host] < self._COOLDOWN_DURATION:
				continue

			# finally, return a peer to connect
			yield peer


class PeerDisconnectedEC(EntityComponent):
	pass
