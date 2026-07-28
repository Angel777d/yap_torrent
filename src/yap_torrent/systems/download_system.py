import logging
import random
from typing import Dict, Generator, Optional, Set

from angelovich.core.DataStorage import Entity

from yap_torrent.components.common import IdleEC
from yap_torrent.components.peer_ec import LocalInterestedEC, PeerConnectionEC, PeerEC, PeerRateEC, PeerStatsEC, \
	RemoteUnchokedEC
from yap_torrent.components.piece_ec import CompletePieceDataEC, PieceDownloadProgressEC, PieceEC
from yap_torrent.components.torrent_ec import ActiveTorrentEC, TorrentEC, TorrentInfoEC, TorrentStatsEC
from yap_torrent.env import Env
from yap_torrent.protocol import bt_main_messages as msg
from yap_torrent.protocol.message import Message
from yap_torrent.protocol.structures import PieceBlockInfo
from yap_torrent.system import System
from yap_torrent.systems import get_info_hash, is_torrent_complete, iterate_connected_peers
from yap_torrent.systems.intrest_system import interested_pieces
from yap_torrent.utils import check_hash

logger = logging.getLogger(__name__)

PIPELINE = 10  # outstanding block requests kept in flight per peer


class DownloadSystem(System):

	async def start(self):
		self.add_listener("peer.message", self.__on_message)
		self.add_listener("peer.local.choked_changed", self._on_ready)
		self.add_listener("peer.local.interested_changed", self._on_ready)
		self.add_listener("peer.disconnected", self._on_peer_disconnected)

	async def _update(self, delta_time: float):
		# keep each active torrent's ready peers requesting. This is the safety net that
		# starts downloads when a torrent is promoted into the active window and picks up
		# blocks freed by a disconnect — cases with no peer event to react to. It's cheap
		# when pipelines are full (_request_from_peer no-ops).
		for torrent_entity in list(self.env.data_storage.get_collection(ActiveTorrentEC)):
			for peer_entity in list(iterate_connected_peers(self.env, get_info_hash(torrent_entity))):
				await _request_from_peer(self.env, torrent_entity, peer_entity)

	async def __on_message(self, torrent_entity: Entity, peer_entity: Entity, message: Message):
		if message.message_id != msg.MessageId.PIECE.value:
			return
		await _process_piece_message(self.env, peer_entity, torrent_entity, message)

	async def _on_ready(self, torrent_entity: Entity, peer_entity: Entity):
		await _request_from_peer(self.env, torrent_entity, peer_entity)

	async def _on_peer_disconnected(self, torrent_entity: Entity, peer_entity: Entity):
		_release_peer_blocks(self.env, torrent_entity, peer_entity)


def _iter_progress_pieces(env: Env, info_hash: bytes) -> Generator[Entity, None, None]:
	for e in env.data_storage.get_collection(PieceDownloadProgressEC):
		if e.get_component(PieceEC).info_hash == info_hash:
			yield e


def _release_peer_blocks(env: Env, torrent_entity: Entity, peer_entity: Entity) -> None:
	"""Return a departing peer's in-flight blocks to the pool so others re-request them."""
	if not peer_entity.has_component(PeerConnectionEC):
		return
	info_hash = get_info_hash(torrent_entity)
	conn = peer_entity.get_component(PeerConnectionEC)
	for block in list(conn.requested):
		piece_entity = env.data_storage.get_collection(PieceEC).find(PieceEC.make_hash(info_hash, block.index))
		if piece_entity is not None and piece_entity.has_component(PieceDownloadProgressEC):
			piece_entity.get_component(PieceDownloadProgressEC).release(block)
	conn.requested.clear()
	for piece_entity in _iter_progress_pieces(env, info_hash):
		piece_entity.get_component(PieceDownloadProgressEC).downloading_by.discard(peer_entity)


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

	info_hash = get_info_hash(torrent_entity)
	counters: Dict[int, int] = {index: 0 for index in pieces}
	for peer_entity in env.data_storage.get_collection(PeerConnectionEC):
		if peer_entity.get_component(PeerEC).info_hash != info_hash:
			continue
		for index in peer_entity.get_component(PeerEC).remote_bitfield.have.intersection(pieces):
			counters[index] = counters.get(index, 0) + 1

	return sorted(counters.items(), key=lambda x: x[1]).pop(0)[0]


def _next_block(env: Env, torrent_entity: Entity, peer_entity: Entity) -> Optional[PieceBlockInfo]:
	remote_bitfield = peer_entity.get_component(PeerEC).remote_bitfield
	interested = interested_pieces(torrent_entity, remote_bitfield)
	if not interested:
		return None

	info_hash = get_info_hash(torrent_entity)

	# 1) continue an already in-progress piece this peer can serve
	for piece_entity in _iter_progress_pieces(env, info_hash):
		index = piece_entity.get_component(PieceEC).info.index
		if index not in interested:
			continue
		progress = piece_entity.get_component(PieceDownloadProgressEC)
		block = progress.next_block()
		if block is not None:
			progress.mark_requested(block)
			progress.downloading_by.add(peer_entity)
			return block

	# 2) start a new piece (rarest-first among pieces not yet in progress)
	in_progress = {e.get_component(PieceEC).info.index for e in _iter_progress_pieces(env, info_hash)}
	candidates = interested - in_progress
	if candidates:
		index = _find_rarest(env, torrent_entity, candidates)
		piece_entity = _get_or_create_piece(env, torrent_entity, index)
		progress = piece_entity.get_component(PieceDownloadProgressEC)
		block = progress.next_block()
		if block is not None:
			progress.mark_requested(block)
			progress.downloading_by.add(peer_entity)
			return block

	# 3) endgame: every wanted piece is already in progress and fully requested —
	#    re-request a not-yet-received block from this peer too (redundancy)
	conn = peer_entity.get_component(PeerConnectionEC)
	for piece_entity in _iter_progress_pieces(env, info_hash):
		index = piece_entity.get_component(PieceEC).info.index
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
	# only active-window torrents initiate new requests, and only from ready peers
	if not torrent_entity.has_component(ActiveTorrentEC):
		return
	if not (peer_entity.has_component(LocalInterestedEC) and peer_entity.has_component(RemoteUnchokedEC)):
		return
	if not peer_entity.has_component(PeerConnectionEC):
		return

	conn = peer_entity.get_component(PeerConnectionEC)
	while len(conn.requested) < PIPELINE:
		block = _next_block(env, torrent_entity, peer_entity)
		if block is None:
			break
		conn.requested.add(block)
		await conn.request(block)


async def _process_piece_message(env: Env, peer_entity: Entity, torrent_entity: Entity, message: Message):
	if is_torrent_complete(torrent_entity):
		return

	index, begin, block = msg.payload_piece(message)

	peer_entity.get_component(PeerStatsEC).add_downloaded(len(block))
	peer_entity.get_component(PeerRateEC).add_downloaded(len(block))
	torrent_entity.get_component(TorrentStatsEC).update_downloaded(len(block))

	conn = peer_entity.get_component(PeerConnectionEC)
	conn.requested.discard(PieceBlockInfo(index, begin, len(block)))

	info_hash = get_info_hash(torrent_entity)
	piece_entity = env.data_storage.get_collection(PieceEC).find(PieceEC.make_hash(info_hash, index))
	if piece_entity is None or not piece_entity.has_component(PieceDownloadProgressEC):
		return

	progress = piece_entity.get_component(PieceDownloadProgressEC)

	# endgame: cancel this exact block on other peers redundantly requesting it
	for other in list(progress.downloading_by):
		if other is peer_entity or not other.has_component(PeerConnectionEC):
			continue
		other_conn = other.get_component(PeerConnectionEC)
		for pending in [b for b in other_conn.requested if b.index == index and b.begin == begin]:
			other_conn.requested.discard(pending)
			await other_conn.send(msg.cancel(pending.index, pending.begin, pending.length))

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
		piece_entity.remove_component(PieceDownloadProgressEC)
		piece_entity.add_component(PieceDownloadProgressEC(progress.info))
		return

	downloading_by = set(progress.downloading_by)
	piece_entity.remove_component(PieceDownloadProgressEC)
	piece_entity.add_component(CompletePieceDataEC(data))
	piece_entity.add_component(IdleEC())
	torrent_entity.get_component(TorrentEC).bitfield.set_index(index)

	await env.event_bus.dispatch_async("piece.complete", torrent_entity, piece_entity)

	# cancel this piece on the other peers that were downloading it
	for other in downloading_by:
		if other is peer_entity or not other.has_component(PeerConnectionEC):
			continue
		other_conn = other.get_component(PeerConnectionEC)
		for pending in [b for b in other_conn.requested if b.index == index]:
			other_conn.requested.discard(pending)
			await other_conn.send(msg.cancel(pending.index, pending.begin, pending.length))
