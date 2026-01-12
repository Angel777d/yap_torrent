import asyncio
import logging
import random
from functools import partial
from typing import Set

from angelovich.core.DataStorage import Entity, DataStorage

from yap_torrent.components.bitfield_ec import BitfieldEC
from yap_torrent.components.peer_ec import PeerConnectionEC, PeerInfoEC
from yap_torrent.components.piece_ec import PieceEC, PiecePendingRemoveEC
from yap_torrent.components.torrent_ec import TorrentHashEC, TorrentInfoEC, TorrentStatsEC, TorrentDownloadEC
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

	async def _on_peer_removed(self, peer_entity: Entity):
		peer_ec = peer_entity.get_component(PeerInfoEC)
		torrent_entity = self.env.data_storage.get_collection(TorrentHashEC).find(peer_ec.info_hash)
		if torrent_entity.has_component(TorrentDownloadEC):
			torrent_entity.get_component(TorrentDownloadEC).cancel(peer_ec.get_hash())

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

		if not torrent_entity.has_component(TorrentInfoEC):
			return

		if not torrent_entity.has_component(TorrentDownloadEC):
			info = torrent_entity.get_component(TorrentInfoEC).info
			callback = partial(_find_rarest, self.env, torrent_entity)
			torrent_entity.add_component(TorrentDownloadEC(info, callback))

		await _request_next(torrent_entity, peer_entity)

	async def _stop_download(self, torrent_entity: Entity, peer_entity: Entity):
		if torrent_entity.has_component(TorrentDownloadEC):
			torrent_entity.get_component(TorrentDownloadEC).cancel(peer_entity.get_component(PeerInfoEC).get_hash())
		logger.debug("%s stop download", peer_entity.get_component(PeerConnectionEC))


def _get_piece_entity(ds: DataStorage, torrent_entity: Entity, index: int) -> Entity:
	info_hash = torrent_entity.get_component(TorrentHashEC).info_hash
	piece_entity = ds.get_collection(PieceEC).find(PieceEC.make_hash(info_hash, index))
	if not piece_entity:
		piece_info = torrent_entity.get_component(TorrentInfoEC).info.get_piece_info(index)
		piece_entity = ds.create_entity().add_component(PieceEC(info_hash, piece_info))
	return piece_entity


def _complete_piece(env: Env, torrent_entity: Entity, index: int, data: bytes) -> Entity:
	logger.debug("Piece %s completed", index)

	# crate piece entity
	info_hash = torrent_entity.get_component(TorrentHashEC).info_hash
	piece_info = torrent_entity.get_component(TorrentInfoEC).info.get_piece_info(index)
	piece_ec = PieceEC(info_hash, piece_info)
	piece_ec.set_data(data)
	piece_entity = env.data_storage.create_entity()
	piece_entity.add_component(piece_ec)
	piece_entity.add_component(PiecePendingRemoveEC())

	# update bitfield
	torrent_entity.get_component(BitfieldEC).set_index(index)

	return piece_entity


async def _process_piece_message(env: Env, peer_entity: Entity, torrent_entity: Entity, message: Message):
	index, begin, block = msg.payload_piece(message)
	# update stats
	torrent_entity.get_component(TorrentStatsEC).update_downloaded(len(block))

	blocks_manager = torrent_entity.get_component(TorrentDownloadEC)

	# save block data
	peer_hash = peer_entity.get_component(PeerInfoEC).get_hash()
	block_info = PieceBlockInfo(index, begin, len(block))
	blocks_manager.set_block_data(block_info, block, peer_hash)

	# ready to save a piece
	if blocks_manager.is_completed(index):
		data = blocks_manager.get_piece_data(index)
		if data:
			piece_entity = _complete_piece(env, torrent_entity, index, data)
			# wait for all systems to finish
			await asyncio.gather(*env.event_bus.dispatch("piece.complete", torrent_entity, piece_entity))
		else:
			# nothing at the moment
			pass

	if is_torrent_complete(torrent_entity):
		torrent_entity.remove_component(TorrentDownloadEC)
		env.event_bus.dispatch("action.torrent.complete", torrent_entity)
		return

	# load next blocks
	await _request_next(torrent_entity, peer_entity)


def _find_rarest(env: Env, torrent_entity: Entity, pieces: Set[int]) -> int:
	# TODO: implement rarest first strategy
	return random.choice(list(pieces))


async def _request_next(torrent_entity: Entity, peer_entity: Entity) -> None:
	local_bitfield = torrent_entity.get_component(BitfieldEC)
	remote_bitfield = peer_entity.get_component(BitfieldEC)
	interested_in = local_bitfield.interested_in(remote_bitfield)

	blocks_manager = torrent_entity.get_component(TorrentDownloadEC)
	peer_hash = peer_entity.get_component(PeerInfoEC).get_hash()
	for block in blocks_manager.request_blocks(interested_in, peer_hash):
		await peer_entity.get_component(PeerConnectionEC).request(block)
