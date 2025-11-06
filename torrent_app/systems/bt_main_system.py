import logging
import random
from pathlib import Path

from angelovichcore.DataStorage import Entity
from torrent_app import System, Env
from torrent_app.components.bitfield_ec import BitfieldEC
from torrent_app.components.peer_ec import PeerConnectionEC, PeerInfoEC
from torrent_app.components.piece_ec import PieceEC, PiecePendingRemoveEC
from torrent_app.components.torrent_ec import TorrentHashEC, TorrentInfoEC
from torrent_app.components.tracker_ec import TorrentTrackerDataEC
from torrent_app.protocol import bt_main_messages as msg
from torrent_app.protocol.connection import Message, Connection
from torrent_app.utils import load_piece, check_hash

logger = logging.getLogger(__name__)


class BTMainSystem(System):
	async def start(self) -> 'System':
		self.env.event_bus.add_listener("peer.message", self.__on_message, scope=self)
		return await super().start()

	def close(self):
		self.env.event_bus.remove_all(scope=self)
		super().close()

	async def __on_message(self, torrent_entity: Entity, peer_entity: Entity, message: Message):
		# do not process ext messages here
		if message.message_id > msg.MessageId.CANCEL.value:
			return

		env = self.env
		bitfield_ec = peer_entity.get_component(BitfieldEC)
		peer_connection_ec = peer_entity.get_component(PeerConnectionEC)
		peer_id = peer_connection_ec.connection.remote_peer_id

		message_id = msg.MessageId(message.message_id)

		if message_id == msg.MessageId.PIECE:
			await _process_piece_message(env, peer_entity, torrent_entity, message)
		elif message_id == msg.MessageId.REQUEST:
			await _process_request_message(env, peer_entity, torrent_entity, message)
		elif message_id == msg.MessageId.HAVE:
			bitfield_ec.set_index(msg.payload_index(message))
			await _update_interested(peer_entity, torrent_entity)
			if peer_connection_ec.is_free_to_download():
				await _update_download(env, peer_entity, torrent_entity)
		elif message_id == msg.MessageId.CHOKE:
			peer_connection_ec.local_choked = True
			logger.debug(f"peer {peer_id} choked us")
		elif message_id == msg.MessageId.UNCHOKE:
			if peer_connection_ec.local_choked:
				peer_connection_ec.local_choked = False
				await _update_download(env, peer_entity, torrent_entity)
				# TODO: fix choke algorythm
				# await peer_connection_ec.unchoke()
				pass
		elif message_id == msg.MessageId.INTERESTED:
			# TODO: fix choke algorythm
			peer_connection_ec.remote_interested = True
			await peer_connection_ec.unchoke()
		elif message_id == msg.MessageId.NOT_INTERESTED:
			# TODO: fix choke algorythm
			peer_connection_ec.remote_interested = False
			await peer_connection_ec.choke()
		elif message_id == msg.MessageId.BITFIELD:
			bitfield_ec.update(msg.payload_bitfield(message))
			await _update_interested(peer_entity, torrent_entity)
			await _update_download(env, peer_entity, torrent_entity)


async def _update_download(env: Env, peer_entity: Entity, torrent_entity: Entity):
	ds = env.data_storage
	peer_connection_ec = peer_entity.get_component(PeerConnectionEC)
	peer_id = peer_connection_ec.connection.remote_peer_id

	# check if interested
	if not peer_connection_ec.local_interested:
		return

	info_hash = torrent_entity.get_component(TorrentHashEC).info_hash
	info = torrent_entity.get_component(TorrentInfoEC).info

	# check if choked
	if peer_connection_ec.local_choked:
		# stop wait if choked
		return

	if not peer_connection_ec.is_free_to_download():
		logger.warning("peer is in download already")
		return

	# TODO: implement strategies
	local_bitfield = torrent_entity.get_component(BitfieldEC)
	remote_bitfield = peer_entity.get_component(BitfieldEC)
	pieces = local_bitfield.interested_in(remote_bitfield, exclude=set())
	index = random.choice(list(pieces))

	logger.debug(f"selected piece {index} for {torrent_entity.get_component(TorrentInfoEC).info.name}. peer {peer_id}")
	# find or create piece
	piece_entity = ds.get_collection(PieceEC).find(PieceEC.make_hash(info_hash, index))
	if not piece_entity:
		piece_entity = ds.create_entity().add_component(PieceEC(info_hash, info, index))
		piece_entity.add_component(PiecePendingRemoveEC())

	await _request_pieces(peer_connection_ec, piece_entity.get_component(PieceEC))


async def _update_interested(peer_entity: Entity, torrent_entity: Entity):
	if not torrent_entity.has_component(TorrentInfoEC):
		return

	remote_bitfield = peer_entity.get_component(BitfieldEC)
	local_bitfield = torrent_entity.get_component(BitfieldEC)
	connection = peer_entity.get_component(PeerConnectionEC)

	if local_bitfield.interested_in(remote_bitfield, exclude=set()):
		await connection.interested()
	else:
		await connection.not_interested()


async def _request_pieces(peer_connection_ec: PeerConnectionEC, piece_ec: PieceEC) -> None:
	in_progress = peer_connection_ec.get_blocks(piece_ec.index)
	while peer_connection_ec.can_request() and piece_ec.has_next(in_progress):
		index, begin, length = piece_ec.get_next(in_progress)
		await peer_connection_ec.request(index, begin, length)
		in_progress.add(begin)


async def _send_have_to_peers(env: Env, info_hash: bytes, index: int):
	entities = env.data_storage.get_collection(PeerConnectionEC).entities
	for entity in entities:
		connection: Connection = entity.get_component(PeerConnectionEC).connection
		peer_ec: PeerInfoEC = entity.get_component(PeerInfoEC)
		if peer_ec.info_hash == info_hash:
			await connection.send(msg.have(index))


async def _process_piece_message(env: Env, peer_entity: Entity, torrent_entity: Entity, message: Message):
	ds = env.data_storage
	info_hash = torrent_entity.get_component(TorrentHashEC).info_hash
	peer_connection_ec = peer_entity.get_component(PeerConnectionEC)

	index, begin, block = msg.payload_piece(message)

	# add block to piece
	piece_entity = ds.get_collection(PieceEC).find(PieceEC.make_hash(info_hash, index))
	piece_ec = piece_entity.get_component(PieceEC)

	# check piece already completed (by other peer)
	if piece_ec.completed:
		await _update_interested(peer_entity, torrent_entity)
		if peer_connection_ec.is_free_to_download():
			await _update_download(env, peer_entity, torrent_entity)
		return

	# other peer can finish this first
	if begin in peer_connection_ec.get_blocks(index):
		# add data to piece
		piece_ec.append(begin, block)

		# remove block from connection queue
		peer_connection_ec.reset_block(index, begin)

		# cancel others
		for entity in ds.get_collection(PeerConnectionEC).entities:
			p = entity.get_component(PeerConnectionEC)
			if begin in p.get_blocks(index):
				logger.debug(f"cancel download block {index, begin} {p.connection.remote_peer_id}")
				p.reset_block(index, begin)
				await p.connection.send(msg.cancel(index, begin, len(block)))

	# whole piece was downloaded
	if piece_ec.completed:
		logger.info(f"piece {index} completed")

		torrent_entity.get_component(BitfieldEC).set_index(index)
		await _send_have_to_peers(env, info_hash, index)

		await _update_interested(peer_entity, torrent_entity)

	# request for next pieces
	else:
		await _request_pieces(peer_connection_ec, piece_ec)

	# go to next piece
	if peer_connection_ec.is_free_to_download():
		await _update_download(env, peer_entity, torrent_entity)


async def _process_request_message(env: Env, peer_entity: Entity, torrent_entity: Entity, message: Message):
	ds = env.data_storage
	config = env.config
	info_hash = torrent_entity.get_component(TorrentHashEC).info_hash
	torrent_info = torrent_entity.get_component(TorrentInfoEC).info
	connection = peer_entity.get_component(PeerConnectionEC).connection

	index, begin, length = msg.payload_request(message)

	piece_entity = ds.get_collection(PieceEC).find(PieceEC.make_hash(info_hash, index))

	# load piece
	if not piece_entity:
		root = Path(config.download_folder)
		data = load_piece(root, torrent_info, index)

		piece_entity = ds.create_entity().add_component(PieceEC(info_hash, torrent_info, index, data))
		piece_entity.add_component(PiecePendingRemoveEC())

	piece_ec = piece_entity.get_component(PieceEC)
	if not piece_ec.completed:
		logger.error(f"Piece {index} in {torrent_info.name} is not completed on request")
		# TODO: how did we get here?
		return

	if not check_hash(piece_ec.data, torrent_info.pieces.get_piece_hash(index)):
		logger.error(f"Piece {index} in {torrent_info.name} torrent is broken")
		# TODO: check files, reload piece
		return

	data = piece_ec.get_block(begin, length)
	piece_entity.get_component(PiecePendingRemoveEC).update()
	torrent_entity.get_component(TorrentTrackerDataEC).update_uploaded(length)
	await connection.send(msg.piece(index, begin, data))
