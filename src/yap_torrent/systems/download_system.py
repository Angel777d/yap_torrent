import logging
import random
import time
from typing import Dict, Iterable, List, Optional, Set

from angelovich.core.DataStorage import Entity

from yap_torrent.components.common import IdleEC
from yap_torrent.components.peer_ec import LocalInterestedEC, PeerConnectionEC, PeerEC, PeerRateEC, PeerStatsEC, \
	RemoteUnchokedEC
from yap_torrent.components.piece_ec import CompletePieceDataEC, PieceDownloadProgressEC, PieceEC
from yap_torrent.components.torrent_ec import TorrentEC, TorrentInfoEC, TorrentStatsEC
from yap_torrent.env import Env
from yap_torrent.protocol import bt_main_messages as msg
from yap_torrent.protocol.message import Message
from yap_torrent.protocol.structures import PieceBlockInfo
from yap_torrent.system import TimeSystem
from yap_torrent.systems import get_info_hash, get_torrent_entity, is_torrent_complete, iterate_connected_peers
from yap_torrent.systems.intrest_system import interested_pieces
from yap_torrent.utils import check_hash

logger = logging.getLogger(__name__)

PIPELINE = 10  # outstanding block requests kept in flight per peer
EXPIRE_TICK = 10  # seconds between sweeps for requests a peer never answered


class DownloadSystem(TimeSystem):
	"""Requests blocks and assembles pieces.

	Requesting is driven entirely by events — the tick is *only* the reclaim sweep for
	requests that were accepted and never answered, which no event can tell us about.
	"""

	def __init__(self, env: Env):
		super().__init__(env, EXPIRE_TICK)

	async def start(self):
		self.add_listener("peer.message", self.__on_message)
		self.add_listener("peer.local.choked_changed", self._on_choked_changed)
		self.add_listener("peer.local.interested_changed", self._on_ready)
		self.add_listener("peer.disconnected", self._on_peer_disconnected)

	async def _update(self, delta_time: float):
		await _expire_requests(self.env)

	async def __on_message(self, torrent_entity: Entity, peer_entity: Entity, message: Message):
		if message.message_id != msg.MessageId.PIECE.value:
			return
		await _process_piece_message(self.env, peer_entity, torrent_entity, message)

	async def _on_ready(self, torrent_entity: Entity, peer_entity: Entity):
		await _request_from_peer(self.env, torrent_entity, peer_entity)

	async def _on_choked_changed(self, torrent_entity: Entity, peer_entity: Entity):
		if peer_entity.has_component(RemoteUnchokedEC):
			await _request_from_peer(self.env, torrent_entity, peer_entity)
			return

		# CHOKE: the peer discards our pending requests on its side (BEP-3), so they will
		# never be answered. Left in the pipeline they would keep it full for the rest of
		# the connection — the peer would never be asked for anything again after its
		# first choke — and their blocks would stay marked as requested for everyone else.
		_release_peer_blocks(self.env, torrent_entity, peer_entity)
		await _fill_peers(self.env, torrent_entity, skip=peer_entity)

	async def _on_peer_disconnected(self, torrent_entity: Entity, peer_entity: Entity):
		"""Free the departing peer's blocks, then offer them to whoever is left.

		Not a stall fix — endgame keeps an in-queue peer fed with redundant blocks either
		way. It just stops the freed blocks waiting for those redundant requests to drain
		before anyone picks them up.
		"""
		_release_peer_blocks(self.env, torrent_entity, peer_entity)
		await _fill_peers(self.env, torrent_entity, skip=peer_entity)


def _progress_by_index(env: Env, info_hash: bytes) -> Dict[int, Entity]:
	"""The torrent's in-progress pieces, keyed by index.

	Built once per pipeline fill: the underlying collection spans every torrent, and
	walking it per block made requesting cost O(pipeline x pieces in flight).
	"""
	result: Dict[int, Entity] = {}
	for entity in env.data_storage.get_collection(PieceDownloadProgressEC):
		piece_ec = entity.get_component(PieceEC)
		if piece_ec.info_hash == info_hash:
			result[piece_ec.info.index] = entity
	return result


def _release_block(env: Env, info_hash: bytes, block: PieceBlockInfo) -> None:
	"""Return a still-unreceived block to the pool so another peer can ask for it."""
	piece_entity = env.data_storage.get_collection(PieceEC).find(PieceEC.make_hash(info_hash, block.index))
	if piece_entity is not None and piece_entity.has_component(PieceDownloadProgressEC):
		piece_entity.get_component(PieceDownloadProgressEC).release(block)


def _release_peer_blocks(env: Env, torrent_entity: Entity, peer_entity: Entity) -> None:
	"""Empty a peer's pipeline and return everything in it to the pool."""
	if not peer_entity.has_component(PeerConnectionEC):
		return
	info_hash = get_info_hash(torrent_entity)
	conn = peer_entity.get_component(PeerConnectionEC)
	for block in conn.clear_requests():
		_release_block(env, info_hash, block)
	for piece_entity in _progress_by_index(env, info_hash).values():
		piece_entity.get_component(PieceDownloadProgressEC).downloading_by.discard(peer_entity)


async def _expire_requests(env: Env) -> None:
	"""Reclaim blocks a peer accepted and never answered.

	Nothing else recovers them: a peer holding both queue markers is never dropped as
	idle, so its pipeline stays full and it is never asked again, while the blocks it is
	sitting on stay marked as requested and no one else picks them up outside endgame.
	"""
	timeout = env.config.block_request_timeout
	now = time.monotonic()

	starved: Dict[bytes, List[Entity]] = {}
	for peer_entity in list(env.data_storage.get_collection(PeerConnectionEC)):
		conn = peer_entity.get_component(PeerConnectionEC)
		expired = conn.expired_requests(timeout, now)
		if not expired:
			continue
		logger.debug("%s: reclaiming %d block request(s) older than %ss", conn, len(expired), timeout)
		for block in expired:
			conn.discard_request(block)
			_release_block(env, conn.info_hash, block)
		starved.setdefault(conn.info_hash, []).append(peer_entity)

	for info_hash, peers in starved.items():
		torrent_entity = get_torrent_entity(env, info_hash)
		if torrent_entity is None:
			continue
		# offer the freed blocks to the peers that are answering before handing them
		# back to the ones that just timed out
		for peer_entity in list(iterate_connected_peers(env, info_hash)):
			if peer_entity in peers:
				continue
			await _request_from_peer(env, torrent_entity, peer_entity)
		for peer_entity in peers:
			await _request_from_peer(env, torrent_entity, peer_entity)


def _get_or_create_piece(env: Env, torrent_entity: Entity, index: int) -> Entity:
	ds = env.data_storage
	info_hash = get_info_hash(torrent_entity)
	piece_entity = ds.get_collection(PieceEC).find(PieceEC.make_hash(info_hash, index))
	piece_info = torrent_entity.get_component(TorrentInfoEC).info.get_piece_info(index)
	if piece_entity is None:
		piece_entity = ds.create_entity()
		piece_entity.add_component(PieceEC(info_hash, piece_info))
	if not piece_entity.has_component(PieceDownloadProgressEC) and not piece_entity.has_component(CompletePieceDataEC):
		piece_entity.add_component(PieceDownloadProgressEC(piece_info))
	return piece_entity


def _find_rarest(env: Env, torrent_entity: Entity, pieces: Set[int]) -> int:
	# random-first warm-up so we have something to reciprocate for the choke algorithm
	if torrent_entity.get_component(TorrentEC).bitfield.have_num < 4:
		return random.choice(list(pieces))

	counters: Dict[int, int] = {index: 0 for index in pieces}
	for peer_entity in iterate_connected_peers(env, get_info_hash(torrent_entity)):
		for index in peer_entity.get_component(PeerEC).remote_bitfield.have.intersection(pieces):
			counters[index] += 1

	return min(counters.items(), key=lambda item: item[1])[0]


def _next_block(env: Env, torrent_entity: Entity, peer_entity: Entity,
                interested: Set[int], in_progress: Dict[int, Entity]) -> Optional[PieceBlockInfo]:
	"""Pick the next block to ask this peer for, or None if it has nothing to offer.

	`interested` and `in_progress` are the caller's, and `in_progress` is updated in
	place when a new piece is started — both are stable for the length of one fill.
	"""

	# 1) continue an already in-progress piece this peer can serve
	for index, piece_entity in in_progress.items():
		if index not in interested:
			continue
		progress = piece_entity.get_component(PieceDownloadProgressEC)
		block = progress.next_block()
		if block is not None:
			progress.mark_requested(block)
			progress.downloading_by.add(peer_entity)
			return block

	# 2) start a new piece (rarest-first among pieces not yet in progress)
	candidates = interested.difference(in_progress.keys())
	if candidates:
		index = _find_rarest(env, torrent_entity, candidates)
		piece_entity = _get_or_create_piece(env, torrent_entity, index)
		in_progress[index] = piece_entity
		progress = piece_entity.get_component(PieceDownloadProgressEC)
		block = progress.next_block()
		if block is not None:
			progress.mark_requested(block)
			progress.downloading_by.add(peer_entity)
			return block

	# 3) endgame: every wanted piece is already in progress and fully requested —
	#    re-request a not-yet-received block from this peer too (redundancy)
	conn = peer_entity.get_component(PeerConnectionEC)
	for index, piece_entity in in_progress.items():
		if index not in interested:
			continue
		progress = piece_entity.get_component(PieceDownloadProgressEC)
		for block in progress.missing_blocks():
			if block in conn.requested:
				continue  # this peer is already fetching that block
			progress.downloading_by.add(peer_entity)
			return block
	return None


async def _request_from_peer(env: Env, torrent_entity: Entity, peer_entity: Entity) -> None:
	if not (peer_entity.has_component(LocalInterestedEC) and peer_entity.has_component(RemoteUnchokedEC)):
		return
	if not peer_entity.has_component(PeerConnectionEC):
		return

	conn = peer_entity.get_component(PeerConnectionEC)
	if len(conn.requested) >= PIPELINE:
		return

	interested = interested_pieces(torrent_entity, peer_entity.get_component(PeerEC).remote_bitfield)
	if not interested:
		return

	in_progress = _progress_by_index(env, get_info_hash(torrent_entity))
	while len(conn.requested) < PIPELINE:
		block = _next_block(env, torrent_entity, peer_entity, interested, in_progress)
		if block is None:
			break
		conn.add_request(block)
		await conn.request(block)


async def _fill_peers(env: Env, torrent_entity: Entity, skip: Optional[Entity] = None) -> None:
	"""Offer work to every connected peer of a torrent (used when blocks are freed)."""
	for peer_entity in list(iterate_connected_peers(env, get_info_hash(torrent_entity))):
		if peer_entity is skip:
			continue
		await _request_from_peer(env, torrent_entity, peer_entity)


async def _cancel_on_others(peers: Iterable[Entity], skip: Entity, index: int, begin: Optional[int] = None) -> None:
	"""CANCEL the redundant endgame copies of a block — or of a whole piece, if no begin."""
	for other in peers:
		if other is skip or not other.has_component(PeerConnectionEC):
			continue
		other_conn = other.get_component(PeerConnectionEC)
		for pending in other_conn.pending_for_piece(index):
			if begin is not None and pending.begin != begin:
				continue
			other_conn.discard_request(pending)
			await other_conn.send(msg.cancel(pending.index, pending.begin, pending.length))


async def _process_piece_message(env: Env, peer_entity: Entity, torrent_entity: Entity, message: Message):
	if is_torrent_complete(torrent_entity):
		return

	index, begin, block = msg.payload_piece(message)

	peer_entity.get_component(PeerStatsEC).add_downloaded(len(block))
	peer_entity.get_component(PeerRateEC).add_downloaded(len(block))
	torrent_entity.get_component(TorrentStatsEC).update_downloaded(len(block))

	conn = peer_entity.get_component(PeerConnectionEC)
	conn.take_request(index, begin)

	info_hash = get_info_hash(torrent_entity)
	piece_entity = env.data_storage.get_collection(PieceEC).find(PieceEC.make_hash(info_hash, index))
	if piece_entity is None or not piece_entity.has_component(PieceDownloadProgressEC):
		return

	progress = piece_entity.get_component(PieceDownloadProgressEC)

	# endgame: cancel this exact block on other peers redundantly requesting it
	await _cancel_on_others(list(progress.downloading_by), peer_entity, index, begin)

	if progress.add_block(begin, block):
		await _finish_piece(env, torrent_entity, peer_entity, piece_entity, progress, index)

	if is_torrent_complete(torrent_entity):
		env.event_bus.dispatch("action.torrent.complete", torrent_entity)
		return

	await _request_from_peer(env, torrent_entity, peer_entity)


async def _finish_piece(env, torrent_entity, peer_entity, piece_entity, progress, index):
	data = bytes(progress.data)
	if not check_hash(data, progress.info.piece_hash):
		logger.warning("Piece %s failed validation, re-downloading", index)
		downloading_by = set(progress.downloading_by)
		piece_entity.remove_component(PieceDownloadProgressEC)
		piece_entity.add_component(PieceDownloadProgressEC(progress.info))
		# the fresh progress has no record of what is still in flight elsewhere, so those
		# blocks would arrive against a piece that no longer expects them
		await _cancel_on_others(downloading_by, peer_entity, index)
		return

	downloading_by = set(progress.downloading_by)
	piece_entity.remove_component(PieceDownloadProgressEC)
	piece_entity.add_component(CompletePieceDataEC(data))
	piece_entity.add_component(IdleEC())
	torrent_entity.get_component(TorrentEC).bitfield.set_index(index)

	await env.event_bus.dispatch_async("piece.complete", torrent_entity, piece_entity)

	# cancel this piece on the other peers that were downloading it
	await _cancel_on_others(downloading_by, peer_entity, index)
