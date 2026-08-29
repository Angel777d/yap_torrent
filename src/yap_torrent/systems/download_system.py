import asyncio
import logging
import random
import time
from typing import Dict, Iterable, Optional, Set

from angelovich.core.DataStorage import Entity
from angelovich.core.System import System, TimeSystem

from yap_torrent.components.common import IdleEC
from yap_torrent.components.peer_ec import LocalInterestedEC, PeerConnectionEC, PeerEC, PeerRateEC, PeerRequestsEC, \
	PeerStatsEC, RemoteUnchokedEC
from yap_torrent.components.piece_ec import CompletePieceDataEC, PieceDownloadProgressEC, PieceEC
from yap_torrent.components.torrent_ec import TorrentDownloadProgressEC, TorrentEC, TorrentInfoEC, \
	TorrentPieceAvailabilityEC, TorrentStatsEC
from yap_torrent.env import Env
from yap_torrent.protocol import bt_main_messages as msg
from yap_torrent.protocol.message import Message
from yap_torrent.protocol.structures import PieceBlockInfo
from yap_torrent.systems import get_info_hash, get_torrent_entity, is_torrent_complete, iterate_connected_peers, \
	interested_pieces
from yap_torrent.utils import check_hash

logger = logging.getLogger(__name__)

PIPELINE = 10  # outstanding block requests kept in flight per peer
EXPIRE_TICK = 10  # seconds between sweeps for requests a peer never answered


class DownloadSystem(TimeSystem, System):
	"""Requests blocks and assembles pieces. The tick only reclaims dead requests."""

	def __init__(self, env: Env):
		System.__init__(self, env)
		TimeSystem.__init__(self, EXPIRE_TICK)

	async def start(self):
		self.add_listener("peer.message", self.__on_message)
		self.add_listener("peer.local.choked_changed", self._on_queue_changed)
		self.add_listener("peer.local.interested_changed", self._on_queue_changed)
		self.add_listener("peer.disconnected", self._on_peer_disconnected)
		self.add_listener("action.torrent.files_changed", self._on_files_changed)

	async def _update(self, delta_time: float):
		await _expire_requests(self.env)

	async def __on_message(self, torrent_entity: Entity, peer_entity: Entity, message: Message):
		if message.message_id == msg.MessageId.HAVE.value:
			_availability(torrent_entity).add_have(msg.payload_index(message))
			return
		if message.message_id == msg.MessageId.BITFIELD.value:
			# a peer's whole holding at once — recount rather than trust a delta
			_availability(torrent_entity).invalidate()
			return
		if message.message_id != msg.MessageId.PIECE.value:
			return
		await _process_piece_message(self.env, peer_entity, torrent_entity, message)

	async def _on_files_changed(self, torrent_entity: Entity):
		_availability(torrent_entity).invalidate()

	async def _on_queue_changed(self, torrent_entity: Entity, peer_entity: Entity):
		enter_queue = peer_entity.has_component(RemoteUnchokedEC) and peer_entity.has_component(LocalInterestedEC)

		# nothing changed here. skip
		if peer_entity.has_component(PeerRequestsEC) == enter_queue:
			return

		if enter_queue:
			peer_entity.add_component(PeerRequestsEC())
			_request_from_peer(self.env, torrent_entity, peer_entity)
		else:
			_reset_download_queue(self.env, torrent_entity, peer_entity)

	async def _on_peer_disconnected(self, torrent_entity: Entity, peer_entity: Entity):
		_availability(torrent_entity).invalidate()
		if peer_entity.has_component(PeerRequestsEC):
			_reset_download_queue(self.env, torrent_entity, peer_entity)


def _reset_download_queue(env: Env, torrent_entity: Entity, peer_entity: Entity):
	_release_peer_blocks(env, torrent_entity, peer_entity)
	_fill_peers(env, torrent_entity, skip=peer_entity)
	peer_entity.remove_component(PeerRequestsEC)


def _progress_by_index(env: Env, info_hash: bytes) -> Dict[int, Entity]:
	"""The torrent's in-progress pieces, keyed by index. Built once per pipeline fill."""
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
	info_hash = get_info_hash(torrent_entity)
	for block in peer_entity.get_component(PeerRequestsEC).clear():
		_release_block(env, info_hash, block)
	for piece_entity in _progress_by_index(env, info_hash).values():
		piece_entity.get_component(PieceDownloadProgressEC).downloading_by.discard(peer_entity)


async def _expire_requests(env: Env) -> None:
	timeout = env.config.block_request_timeout
	now = time.monotonic()

	torrents_to_trigger: Set[Entity] = set()
	for peer_entity in env.data_storage.get_collection(PeerRequestsEC):
		requests = peer_entity.get_component(PeerRequestsEC)
		expired = requests.expired(timeout, now)
		if not expired:
			continue

		info_hash = peer_entity.get_component(PeerEC).info_hash
		logger.debug("%s: reclaiming %d block request(s) older than %ss",
		             peer_entity.get_component(PeerEC).peer_info, len(expired), timeout)
		for block in expired:
			requests.discard(block)
			_release_block(env, info_hash, block)

		torrent_entity = get_torrent_entity(env, peer_entity.get_component(PeerEC).info_hash)
		if torrent_entity:
			torrents_to_trigger.add(torrent_entity)

	for torrent_entity in torrents_to_trigger:
		_fill_peers(env, torrent_entity)


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


def _availability(torrent_entity: Entity) -> TorrentPieceAvailabilityEC:
	if not torrent_entity.has_component(TorrentPieceAvailabilityEC):
		torrent_entity.add_component(TorrentPieceAvailabilityEC())
	return torrent_entity.get_component(TorrentPieceAvailabilityEC)


def _wanted_and_missing(torrent_entity: Entity) -> Set[int]:
	"""Pieces this torrent still wants: the wanted mask minus what we already hold."""
	have = torrent_entity.get_component(TorrentEC).bitfield
	if torrent_entity.has_component(TorrentDownloadProgressEC):
		wanted = torrent_entity.get_component(TorrentDownloadProgressEC).wanted.have
	else:
		wanted = set(range(torrent_entity.get_component(TorrentInfoEC).info.pieces_num))
	return wanted.difference(have.have)


def _rarest_order(env: Env, torrent_entity: Entity) -> TorrentPieceAvailabilityEC:
	"""The torrent's availability order, recounted only if the swarm moved since last time."""
	availability = _availability(torrent_entity)
	if availability.needs_rebuild:
		holdings = (peer.get_component(PeerEC).remote_bitfield.have
		            for peer in iterate_connected_peers(env, get_info_hash(torrent_entity)))
		availability.rebuild(holdings, _wanted_and_missing(torrent_entity))
	return availability


def _find_rarest(env: Env, torrent_entity: Entity, pieces: Set[int]) -> int:
	# random-first warm-up so we have something to reciprocate for the choke algorithm
	if torrent_entity.get_component(TorrentEC).bitfield.have_num < 4:
		return random.choice(list(pieces))

	return _rarest_order(env, torrent_entity).rarest_of(pieces)


def _next_block(env: Env, torrent_entity: Entity, peer_entity: Entity,
                interested: Set[int], in_progress: Dict[int, Entity]) -> Optional[PieceBlockInfo]:
	"""Pick the next block to ask this peer for, or None if it has nothing to offer."""

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

	# 3) endgame: re-request a not-yet-received block redundantly
	in_flight = peer_entity.get_component(PeerRequestsEC).blocks
	for index, piece_entity in in_progress.items():
		if index not in interested:
			continue
		progress = piece_entity.get_component(PieceDownloadProgressEC)
		for block in progress.missing_blocks():
			if block in in_flight:
				continue  # this peer is already fetching that block
			progress.downloading_by.add(peer_entity)
			return block
	return None


def _request_from_peer(env: Env, torrent_entity: Entity, peer_entity: Entity) -> None:
	conn = peer_entity.get_component(PeerConnectionEC)
	requests = peer_entity.get_component(PeerRequestsEC)
	if len(requests.blocks) >= PIPELINE:
		return

	interested = interested_pieces(torrent_entity, peer_entity.get_component(PeerEC).remote_bitfield)
	if not interested:
		return

	in_progress = _progress_by_index(env, get_info_hash(torrent_entity))
	while len(requests.blocks) < PIPELINE:
		block = _next_block(env, torrent_entity, peer_entity, interested, in_progress)
		if block is None:
			break

		requests.add(block, asyncio.create_task(conn.request(block)))


def _fill_peers(env: Env, torrent_entity: Entity, skip: Optional[Entity] = None) -> None:
	"""Offer work to every connected peer of a torrent (used when blocks are freed)."""
	for peer_entity in env.data_storage.get_collection(PeerRequestsEC):
		if peer_entity is skip:
			continue
		if torrent_entity.get_component(TorrentEC).info_hash != peer_entity.get_component(PeerEC).info_hash:
			continue
		_request_from_peer(env, torrent_entity, peer_entity)


async def _cancel_on_others(peers: Iterable[Entity], index: int, begin: Optional[int] = None) -> None:
	"""CANCEL the redundant endgame copies of a block — or of a whole piece, if no begin."""
	for other in peers:
		if not other.has_component(PeerRequestsEC) or not other.has_component(PeerConnectionEC):
			continue  # left the queue while an earlier CANCEL was in flight
		other_requests = other.get_component(PeerRequestsEC)
		other_conn = other.get_component(PeerConnectionEC)
		for pending in other_requests.for_piece(index):
			if begin is not None and pending.begin != begin:
				continue
			other_requests.discard(pending)
			await other_conn.send(msg.cancel(pending.index, pending.begin, pending.length))


async def _process_piece_message(env: Env, peer_entity: Entity, torrent_entity: Entity, message: Message):
	if is_torrent_complete(torrent_entity):
		return

	index, begin, block = msg.payload_piece(message)

	peer_entity.get_component(PeerStatsEC).add_downloaded(len(block))
	peer_entity.get_component(PeerRateEC).add_downloaded(len(block))
	torrent_entity.get_component(TorrentStatsEC).update_downloaded(len(block))

	peer_entity.get_component(PeerRequestsEC).take(index, begin)

	info_hash = get_info_hash(torrent_entity)
	piece_entity = env.data_storage.get_collection(PieceEC).find(PieceEC.make_hash(info_hash, index))
	if piece_entity is None or not piece_entity.has_component(PieceDownloadProgressEC):
		return

	progress = piece_entity.get_component(PieceDownloadProgressEC)

	# endgame: cancel this exact block on other peers redundantly requesting it
	await _cancel_on_others(progress.downloading_by.difference([peer_entity]), index, begin)

	if progress.add_block(begin, block):
		await _finish_piece(env, torrent_entity, piece_entity, progress, index)

	if is_torrent_complete(torrent_entity):
		env.event_bus.dispatch("action.torrent.complete", torrent_entity)
		return

	# finish last piece can trigger PeerRequestsEC remove
	if peer_entity.has_component(PeerRequestsEC):
		# request next block
		_request_from_peer(env, torrent_entity, peer_entity)


async def _finish_piece(env: Env, torrent_entity: Entity, piece_entity: Entity, progress: PieceDownloadProgressEC,
                        index: int):
	data = bytes(progress.data)
	piece_entity.remove_component(PieceDownloadProgressEC)

	if not check_hash(data, progress.info.piece_hash):
		logger.warning("Piece %s failed validation, re-downloading", index)
		piece_entity.add_component(PieceDownloadProgressEC(progress.info))
		return

	piece_entity.add_component(CompletePieceDataEC(data))
	piece_entity.add_component(IdleEC())
	torrent_entity.get_component(TorrentEC).bitfield.set_index(index)
	_availability(torrent_entity).drop(index)  # held now, so no longer worth ordering

	await env.event_bus.dispatch_async("piece.complete", torrent_entity, piece_entity)
