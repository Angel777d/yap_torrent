import asyncio
import logging
import random
from functools import partial
from typing import Set, Dict

from angelovich.core.DataStorage import Entity, DataStorage

from yap_torrent.components.peer_ec import PeerConnectionEC
from yap_torrent.components.piece_ec import PieceEC, PiecePendingRemoveEC
from yap_torrent.components.torrent_ec import TorrentEC, TorrentInfoEC, TorrentStatsEC, TorrentDownloadEC
from yap_torrent.env import Env
from yap_torrent.protocol import bt_main_messages as msg
from yap_torrent.protocol.message import Message
from yap_torrent.protocol.structures import PieceBlockInfo
from yap_torrent.system import System
from yap_torrent.systems import is_torrent_complete

logger = logging.getLogger(__name__)


class BTDownloadSystem(System):

	def __init__(self, env: Env):
		super().__init__(env)

	async def start(self):
		self.env.event_bus.add_listener("peer.message", self.__on_message, scope=self)
		self.env.event_bus.add_listener("peer.local.interested_changed", self._on_local_peer_changed, scope=self)
		self.env.event_bus.add_listener("peer.local.choked_changed", self._on_local_peer_changed, scope=self)

	def close(self) -> None:
		self.env.event_bus.remove_all_listeners(scope=self)

	async def __on_message(self, torrent_entity: Entity, peer_entity: Entity, message: Message):
		if message.message_id != msg.MessageId.PIECE.value:
			return
		await _process_piece_message(self.env, peer_entity, torrent_entity, message)

	async def _on_local_peer_changed(self, torrent_entity: Entity, peer_entity: Entity) -> None:
		peer_connection_ec = peer_entity.get_component(PeerConnectionEC)

		if peer_connection_ec.local_interested and not peer_connection_ec.local_choked:
			await self._start_download(torrent_entity, peer_entity)
		else:
			await self._stop_download(torrent_entity, peer_entity)

	async def _start_download(self, torrent_entity: Entity, peer_entity: Entity):
		logger.debug("%s start download", peer_entity.get_component(PeerConnectionEC))
		await _request_next(self.env, torrent_entity, peer_entity)

	async def _stop_download(self, torrent_entity: Entity, peer_entity: Entity):
		logger.debug("%s stop download", peer_entity.get_component(PeerConnectionEC))
		if torrent_entity.has_component(TorrentDownloadEC):
			torrent_entity.get_component(TorrentDownloadEC).cancel(peer_entity.get_component(PeerConnectionEC))


def _get_piece_entity(ds: DataStorage, torrent_entity: Entity, index: int) -> Entity:
	info_hash = torrent_entity.get_component(TorrentEC).info_hash
	piece_entity = ds.get_collection(PieceEC).find(PieceEC.make_hash(info_hash, index))
	if not piece_entity:
		piece_info = torrent_entity.get_component(TorrentInfoEC).info.get_piece_info(index)
		piece_entity = ds.create_entity().add_component(PieceEC(info_hash, piece_info))
	return piece_entity


def _complete_piece(env: Env, torrent_entity: Entity, index: int, data: bytes) -> Entity:
	logger.debug("Piece %s completed", index)

	# crate piece entity
	info_hash = torrent_entity.get_component(TorrentEC).info_hash
	piece_info = torrent_entity.get_component(TorrentInfoEC).info.get_piece_info(index)
	piece_ec = PieceEC(info_hash, piece_info)
	piece_ec.set_data(data)
	piece_entity = env.data_storage.create_entity()
	piece_entity.add_component(piece_ec)
	piece_entity.add_component(PiecePendingRemoveEC())

	# update bitfield
	torrent_entity.get_component(TorrentEC).bitfield.set_index(index)

	return piece_entity


async def _process_piece_message(env: Env, peer_entity: Entity, torrent_entity: Entity, message: Message):
	if is_torrent_complete(torrent_entity):
		return

	index, begin, block = msg.payload_piece(message)
	# update stats
	torrent_entity.get_component(TorrentStatsEC).update_downloaded(len(block))

	blocks_manager = _get_blocks_manager(env, torrent_entity)

	# save block data
	block_info = PieceBlockInfo(index, begin, len(block))
	is_completed, peers_to_cancel = blocks_manager.set_block_data(
		block_info, block, peer_entity.get_component(PeerConnectionEC))

	# ready to save a piece
	if is_completed:
		data = blocks_manager.pop_piece_data(index)
		if data:
			piece_entity = _complete_piece(env, torrent_entity, index, data)
			# wait for all systems to finish
			await asyncio.gather(*env.event_bus.dispatch("piece.complete", torrent_entity, piece_entity))
		else:
			# nothing at the moment
			pass

	# send cancel to peers
	for peer in peers_to_cancel:
		await peer.connection.send(msg.cancel(index, begin, len(block)))

	if is_torrent_complete(torrent_entity):
		torrent_entity.remove_component(TorrentDownloadEC)
		env.event_bus.dispatch("action.torrent.complete", torrent_entity)
		return

	# load next blocks
	await _request_next(env, torrent_entity, peer_entity)


def _find_rarest(env: Env, torrent_entity: Entity, pieces: Set[int]) -> int:
	# random first policy
	# it is important to have some pieces to reciprocate for the choke algorithm
	if torrent_entity.get_component(TorrentEC).bitfield.have_num < 4:
		return random.choice(list(pieces))

	# rarest first strategy
	info_hash = torrent_entity.get_component(TorrentEC).info_hash

	# collect pieces on connected peers
	counters: Dict[int, int] = {index: 0 for index in pieces}
	for peer_entity in env.data_storage.get_collection(PeerConnectionEC):
		peer_ec = peer_entity.get_component(PeerConnectionEC)
		if peer_ec.info_hash != info_hash:
			continue
		for index in peer_ec.remote_bitfield.intersection(pieces):
			counters[index] = counters.get(index, 0) + 1

	# select rarest piece
	index = sorted(counters.items(), key=lambda x: x[1]).pop(0)[0]
	return index


async def _request_next(env: Env, torrent_entity: Entity, peer_entity: Entity) -> None:
	local_bitfield = torrent_entity.get_component(TorrentEC).bitfield
	remote_bitfield = peer_entity.get_component(PeerConnectionEC).remote_bitfield
	interested_in = local_bitfield.interested_in(remote_bitfield)

	blocks_manager = _get_blocks_manager(env, torrent_entity)
	for block in blocks_manager.request_blocks(interested_in, peer_entity.get_component(PeerConnectionEC)):
		await peer_entity.get_component(PeerConnectionEC).request(block)


def _get_blocks_manager(env: Env, torrent_entity):
	if torrent_entity.has_component(TorrentDownloadEC):
		return torrent_entity.get_component(TorrentDownloadEC)
	info = torrent_entity.get_component(TorrentInfoEC).info
	blocks_manager = TorrentDownloadEC(info, partial(_find_rarest, env, torrent_entity))
	torrent_entity.add_component(blocks_manager)
	return blocks_manager
