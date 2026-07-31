import asyncio
import logging
from collections import Counter
from pathlib import Path
from typing import Dict, Optional, Tuple

from angelovich.core.DataStorage import Entity

from yap_torrent.components.common import IdleEC
from yap_torrent.components.peer_ec import LocalUnchokedEC, PeerConnectionEC, PeerEC, PeerRateEC, PeerStatsEC
from yap_torrent.components.piece_ec import CompletePieceDataEC, PieceEC
from yap_torrent.components.torrent_ec import TorrentInfoEC, TorrentStatsEC
from yap_torrent.env import Env
from yap_torrent.protocol import TorrentInfo
from yap_torrent.protocol import bt_main_messages as msg
from yap_torrent.protocol.message import Message
from yap_torrent.system import System
from yap_torrent.systems import get_info_hash, is_torrent_active
from yap_torrent.utils import execute_in_pool, load_and_verify_piece

logger = logging.getLogger(__name__)

MAX_BLOCK_SIZE = 2 ** 14  # 16KiB: the largest block a peer may ask for in one REQUEST
MAX_PENDING_REQUESTS = 100  # per peer — beyond this its REQUESTs are dropped on the floor


def _is_sane_request(info: TorrentInfo, index: int, begin: int, length: int) -> bool:
	"""Whether a REQUEST addresses real bytes of this torrent."""
	if not 0 <= index < info.pieces_num:
		return False
	if not 0 < length <= MAX_BLOCK_SIZE:
		return False
	return 0 <= begin and begin + length <= info.get_piece_info(index).size


class UploadSystem(System):
	"""Answers REQUEST. A reply is served as it arrives; no upload queue, CANCEL ignored."""

	def __init__(self, env: Env):
		super().__init__(env)
		# disk reads in flight, so peers asking for the same uncached piece share one read
		self._loads: Dict[Tuple[bytes, int], asyncio.Task] = {}
		self._pending: Counter = Counter()  # unserved REQUESTs per peer

	async def start(self):
		self.add_listener("peer.message", self.__on_message)
		self.add_listener("peer.disconnected", self._on_peer_disconnected)

	async def __on_message(self, torrent_entity: Entity, peer_entity: Entity, message: Message):
		if message.message_id == msg.MessageId.REQUEST.value:
			await self._on_request(torrent_entity, peer_entity, message)

	async def _on_peer_disconnected(self, _torrent_entity: Entity, peer_entity: Entity):
		self._pending.pop(peer_entity.get_component(PeerEC).key(), None)

	async def _on_request(self, torrent_entity: Entity, peer_entity: Entity, message: Message):
		index, begin, length = msg.payload_request(message)

		if not (self._may_serve(torrent_entity, peer_entity) and torrent_entity.has_component(TorrentInfoEC)):
			return

		info = torrent_entity.get_component(TorrentInfoEC).info
		if not _is_sane_request(info, index, begin, length):
			logger.warning("Dropping bogus request for %s:%s+%s from %s",
			               index, begin, length, peer_entity.get_component(PeerEC).peer_info)
			return

		key = peer_entity.get_component(PeerEC).key()
		if self._pending[key] >= MAX_PENDING_REQUESTS:
			logger.debug("Peer %s is over %s pending requests, dropping", key, MAX_PENDING_REQUESTS)
			return

		self._pending[key] += 1
		try:
			await self._serve(torrent_entity, peer_entity, index, begin, length)
		finally:
			self._pending[key] -= 1
			if self._pending[key] <= 0:
				del self._pending[key]

	async def _serve(self, torrent_entity: Entity, peer_entity: Entity, index: int, begin: int, length: int):
		piece_entity = await self._piece_for_upload(torrent_entity, index)
		if piece_entity is None:
			logger.warning("Peer requested piece %s, which we cannot serve", index)
			return

		# re-check after the (possibly slow) read: peer/torrent may be gone or choked
		if not self._may_serve(torrent_entity, peer_entity):
			return

		piece_entity.get_component(IdleEC).touch()
		data = piece_entity.get_component(CompletePieceDataEC).get_block(begin, length)
		await peer_entity.get_component(PeerConnectionEC).send(msg.piece(index, begin, data))

		# credit what actually went out, not what was asked for
		if peer_entity.is_valid():
			peer_entity.get_component(PeerStatsEC).add_uploaded(len(data))
			peer_entity.get_component(PeerRateEC).add_uploaded(len(data))
		if torrent_entity.is_valid():
			torrent_entity.get_component(TorrentStatsEC).update_uploaded(len(data))

	def _may_serve(self, torrent_entity: Entity, peer_entity: Entity) -> bool:
		if not is_torrent_active(torrent_entity):
			return False
		if not (peer_entity.is_valid() and peer_entity.has_component(PeerConnectionEC)):
			return False
		return peer_entity.has_component(LocalUnchokedEC)

	async def _piece_for_upload(self, torrent_entity: Entity, index: int) -> Optional[Entity]:
		ds = self.env.data_storage
		info_hash = get_info_hash(torrent_entity)

		piece_entity = ds.get_collection(PieceEC).find(PieceEC.make_hash(info_hash, index))
		if piece_entity is not None:
			# a piece we are still downloading holds progress, not data we can serve
			return piece_entity if piece_entity.has_component(CompletePieceDataEC) else None

		info = torrent_entity.get_component(TorrentInfoEC).info
		data = await self._load(info_hash, info, index)
		if data is None:
			return None

		# another request for the same piece may have landed while we were reading
		piece_entity = ds.get_collection(PieceEC).find(PieceEC.make_hash(info_hash, index))
		if piece_entity is not None:
			return piece_entity if piece_entity.has_component(CompletePieceDataEC) else None

		return (ds.create_entity()
		        .add_component(PieceEC(info_hash, info.get_piece_info(index)))
		        .add_component(CompletePieceDataEC(data))
		        .add_component(IdleEC()))

	async def _load(self, info_hash: bytes, info: TorrentInfo, index: int) -> Optional[bytes]:
		"""Read a piece off disk in the process pool, once per piece however many peers ask."""
		key = (info_hash, index)
		task = self._loads.get(key)
		if task is None:
			root = Path(self.env.config.download_folder)
			task = self.add_task(execute_in_pool(load_and_verify_piece, root, info, index))
			self._loads[key] = task
			task.add_done_callback(lambda _task, _key=key: self._loads.pop(_key, None))

		try:
			data = await asyncio.shield(task)
		except asyncio.CancelledError:
			raise
		except Exception as ex:  # noqa: BLE001 — a broken read must not kill the peer's task
			logger.warning("Failed to read piece %s of %s: %s", index, info.name, ex)
			return None

		if data is None:
			logger.error("Piece %s in %s is broken on request", index, info.name)
		return data
