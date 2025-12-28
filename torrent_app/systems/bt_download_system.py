import asyncio
import logging
import random
from typing import Set

from angelovichcore.DataStorage import Entity, DataStorage
from torrent_app import System, Env
from torrent_app.components.bitfield_ec import BitfieldEC
from torrent_app.components.peer_ec import PeerConnectionEC
from torrent_app.components.piece_ec import PieceEC, PieceBlocksEC, PiecePendingRemoveEC
from torrent_app.components.torrent_ec import TorrentHashEC, TorrentInfoEC, TorrentStatsEC, TorrentDownloadEC
from torrent_app.protocol import bt_main_messages as msg
from torrent_app.protocol.message import Message
from torrent_app.protocol.structures import PieceInfo

logger = logging.getLogger(__name__)


class BTDownloadSystem(System):

	def __init__(self, env: Env):
		super().__init__(env)

	async def start(self):
		self.env.event_bus.add_listener("peer.message", self.__on_message, scope=self)
		self.env.event_bus.add_listener("peer.local.interested_changed", _on_local_peer_changed, scope=self)
		self.env.event_bus.add_listener("peer.local.choked_changed", _on_local_peer_changed, scope=self)

	async def __on_message(self, torrent_entity: Entity, peer_entity: Entity, message: Message):
		if message.message_id != msg.MessageId.PIECE.value:
			return
		await _process_piece_message(self.env, peer_entity, torrent_entity, message)


async def _on_local_peer_changed(torrent_entity: Entity, peer_entity: Entity) -> None:
	peer_connection_ec = peer_entity.get_component(PeerConnectionEC)

	if peer_connection_ec.local_interested and not peer_connection_ec.local_choked:
		await _start_download(torrent_entity, peer_entity)
	else:
		await _stop_download(torrent_entity, peer_entity)


def _get_piece_entity(ds: DataStorage, torrent_entity: Entity, index: int) -> Entity:
	info_hash = torrent_entity.get_component(TorrentHashEC).info_hash
	piece_entity = ds.get_collection(PieceEC).find(PieceEC.make_hash(info_hash, index))
	if not piece_entity:
		piece_info = PieceInfo.from_torrent(torrent_entity.get_component(TorrentInfoEC).info, index)
		piece_entity = ds.create_entity().add_component(PieceEC(info_hash, piece_info))
		piece_entity.add_component(PieceBlocksEC(piece_info))
	return piece_entity


async def _complete_piece(env: Env, torrent_entity: Entity, piece_entity: Entity):
	data = piece_entity.get_component(PieceBlocksEC).pull_data_and_reset()
	index = piece_entity.get_component(PieceEC).info.index

	# reset download index in torrent blocks
	torrent_entity.get_component(TorrentDownloadEC).reset_index(index)

	if not piece_entity.get_component(PieceEC).set_data(data):
		logger.info(f"Piece {index} completed")
		return

	logger.debug(f"Piece {index} completed")

	# wait for all systems to finish
	await asyncio.gather(*env.event_bus.dispatch("piece.complete", torrent_entity, piece_entity))

	# remove piece download component
	piece_entity.remove_component(PieceBlocksEC)
	# add a piece management component
	piece_entity.add_component(PiecePendingRemoveEC())


async def _process_piece_message(env: Env, peer_entity: Entity, torrent_entity: Entity, message: Message):
	index, begin, block = msg.payload_piece(message)
	length = len(block)

	peer_connection_ec = peer_entity.get_component(PeerConnectionEC)

	# update stats
	torrent_entity.get_component(TorrentStatsEC).update_downloaded(length)

	# check the peer has the block in downloads
	block_info = peer_connection_ec.find_block(index, begin)
	if not block_info:
		logger.error(f"Block {index} {begin} not found in peer {peer_connection_ec}")
		return

	# clear block references
	peer_connection_ec.complete(block_info)
	torrent_entity.get_component(TorrentDownloadEC).complete(block_info)

	# find or create a piece entity
	piece_entity = _get_piece_entity(env.data_storage, torrent_entity, index)

	# add block to piece
	blocks_ec = piece_entity.get_component(PieceBlocksEC)
	blocks_ec.add_block(block_info, block)

	# ready to save a piece
	if blocks_ec.is_full():
		await _complete_piece(env, torrent_entity, piece_entity)

	# load next blocks
	await _request_next(torrent_entity, peer_entity)


def _find_rarest(pieces: Set[int]) -> int:
	# TODO: implement rarest first strategy
	return random.choice(list(pieces))


async def _start_download(torrent_entity: Entity, peer_entity: Entity):
	logger.info(f"Peer {peer_entity.get_component(PeerConnectionEC).connection.remote_peer_id} start download")
	if not torrent_entity.has_component(TorrentDownloadEC):
		torrent_entity.add_component(TorrentDownloadEC())

	await _request_next(torrent_entity, peer_entity)


async def _stop_download(torrent_entity: Entity, peer_entity: Entity):
	logger.info(f"Peer {peer_entity.get_component(PeerConnectionEC)} stop download")
	blocks = peer_entity.get_component(PeerConnectionEC).reset_downloads()
	for block in blocks:
		torrent_entity.get_component(TorrentDownloadEC).cancel(block)


async def _request_next(torrent_entity: Entity, peer_entity: Entity) -> None:
	peer_connection_ec = peer_entity.get_component(PeerConnectionEC)

	local_bitfield = torrent_entity.get_component(BitfieldEC)
	remote_bitfield = peer_entity.get_component(BitfieldEC)
	interested_in = local_bitfield.interested_in(remote_bitfield, exclude=set())

	while peer_connection_ec.can_request():
		blocks_manager = torrent_entity.get_component(TorrentDownloadEC)
		# find block in the queue
		if blocks_manager.has_blocks(interested_in):
			await peer_connection_ec.request(blocks_manager.next_block(interested_in))
			continue

		# add more blocks to the queue
		new_pieces = blocks_manager.new_pieces(interested_in)
		if new_pieces:
			index = _find_rarest(new_pieces)
			interested_in.remove(index)

			piece_info = PieceInfo.from_torrent(torrent_entity.get_component(TorrentInfoEC).info, index)
			blocks_manager.add_blocks(PieceBlocksEC.create_blocks(piece_info))
			block = blocks_manager.next_block(interested_in)

			await peer_connection_ec.request(block)
			continue

		# nothing to download.
		# TODO: endgame
		break
