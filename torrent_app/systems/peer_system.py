import asyncio
import logging
import random
from asyncio import StreamReader, StreamWriter
from pathlib import Path

from core.DataStorage import Entity
from torrent_app import System, Env
from torrent_app.components.bitfield_ec import BitfieldEC
from torrent_app.components.peer_ec import PeerPendingEC, PeerInfoEC, PeerConnectionEC
from torrent_app.components.piece_ec import PieceEC, PieceToSaveEC, PiecePendingRemoveEC
from torrent_app.components.torrent_ec import TorrentInfoEC
from torrent_app.components.tracker_ec import TorrentTrackerDataEC
from torrent_app.protocol.connection import Connection, MessageId, connect, on_connect, Message
from torrent_app.protocol.structures import PeerInfo
from torrent_app.utils import load_piece, check_hash

logger = logging.getLogger(__name__)


class PeerSystem(System):

	def __init__(self, env: Env):
		super().__init__(env)

	def close(self):
		ds = self.env.data_storage
		ds.clear_collection(PeerConnectionEC)

	async def start(self):
		port = self.env.config.port
		host = self.env.ip
		await asyncio.start_server(self._server_callback, host, port)

		return await super().start()

	async def _server_callback(self, reader: StreamReader, writer: StreamWriter):
		logger.debug('some peer connected to us')
		local_peer_id = self.env.peer_id

		result = await on_connect(local_peer_id, reader, writer)
		if result is None:
			return

		pstrlen, pstr, reserved, info_hash, remote_peer_id = result

		# get peer info from
		host, port = writer.transport.get_extra_info('peername')
		peer_entity = self.env.data_storage.create_entity().add_component(PeerInfoEC(info_hash, PeerInfo(host, 0)))

		logger.info(f'peer {remote_peer_id} is connected to us')

		torrent_entity = self.env.data_storage.get_collection(TorrentInfoEC).find(info_hash)
		if torrent_entity:
			await self._add_peer(peer_entity, info_hash, remote_peer_id, reader, writer)
		else:
			logger.warning(f"no torrent for info hash{info_hash}. handshake: {result}")

	async def _update(self, delta_time: float):
		ds = self.env.data_storage

		# check capacity
		if len(ds.get_collection(PeerConnectionEC)) >= self.env.config.max_connections:
			return

		# sort and filter pending peers
		pending_peers = ds.get_collection(PeerPendingEC).entities
		# TODO: select peers to connect
		pass

		# connect to new peers
		my_peer_id = self.env.peer_id
		active_collection = ds.get_collection(PeerConnectionEC)
		while len(active_collection) < self.env.config.max_connections and pending_peers:
			peer_entity = pending_peers.pop(0)
			peer_entity.remove_component(PeerPendingEC)
			asyncio.create_task(self._connect(peer_entity, my_peer_id))

	async def _connect(self, peer_entity: Entity, my_peer_id: bytes):
		peer_ec: PeerInfoEC = peer_entity.get_component(PeerInfoEC)
		result = await connect(peer_ec.peer_info, peer_ec.info_hash, my_peer_id)
		if not result:
			return
		remote_peer_id, reader, writer = result

		await self._add_peer(peer_entity, peer_ec.info_hash, remote_peer_id, reader, writer)

	async def _add_peer(self, peer_entity: Entity, info_hash: bytes, remote_peer_id: bytes, reader: StreamReader,
	                    writer: StreamWriter):
		ds = self.env.data_storage

		torrent_entity = ds.get_collection(TorrentInfoEC).find(info_hash)
		info = torrent_entity.get_component(TorrentInfoEC).info

		connection = Connection(remote_peer_id, reader, writer)

		task = asyncio.create_task(_listen(self.env, peer_entity, torrent_entity))

		peer_entity.add_component(BitfieldEC(info.pieces.num))
		peer_entity.add_component(PeerConnectionEC(connection, task))


async def _listen(env: Env, peer_entity: Entity, torrent_entity: Entity):
	ds = env.data_storage
	peer_connection_ec = peer_entity.get_component(PeerConnectionEC)
	bitfield_ec = peer_entity.get_component(BitfieldEC)

	connection = peer_connection_ec.connection
	peer_id = connection.remote_peer_id

	# send bitfield first
	local_bitfield = torrent_entity.get_component(BitfieldEC)
	if local_bitfield.have_num > 0:
		await connection.bitfield(local_bitfield.dump())

	# keep alive loop
	# async def keep_alive():
	# 	while True:
	# 		await asyncio.sleep(3)
	# 		await connection.keep_alive()
	#
	# keep_alive_task = asyncio.create_task(keep_alive())

	# main peer loop
	try:
		while not connection.is_dead():
			# await asyncio.sleep(0.1)  # for other peers
			message = await connection.read()

			if message.message_id == MessageId.PIECE:
				await _process_piece_message(env, peer_entity, torrent_entity, message)
			elif message.message_id == MessageId.REQUEST:
				await _process_request_message(env, peer_entity, torrent_entity, message)
			elif message.message_id == MessageId.HAVE:
				bitfield_ec.set_index(message.index)
				await _update_interested(peer_entity, torrent_entity)
				if peer_connection_ec.is_free_to_download():
					await _update_download(env, peer_entity, torrent_entity)
			elif message.message_id == MessageId.CHOKE:
				peer_connection_ec.local_choked = True
				await _clear_download(env, peer_entity, torrent_entity)
			elif message.message_id == MessageId.UNCHOKE:
				if peer_connection_ec.local_choked:
					peer_connection_ec.local_choked = False
					await _update_download(env, peer_entity, torrent_entity)
					# TODO: fix choke algorythm
					# await peer_connection_ec.unchoke()
					pass
			elif message.message_id == MessageId.INTERESTED:
				# TODO: fix choke algorythm
				peer_connection_ec.remote_interested = True
				await peer_connection_ec.unchoke()
			elif message.message_id == MessageId.NOT_INTERESTED:
				# TODO: fix choke algorythm
				peer_connection_ec.remote_interested = False
				await peer_connection_ec.choke()
			elif message.message_id == MessageId.BITFIELD:
				bitfield_ec.update(message.bitfield)
				await _update_interested(peer_entity, torrent_entity)
				await _update_download(env, peer_entity, torrent_entity)
			elif message.message_id == MessageId.ERROR:
				logger.debug(f"got message error on peer {peer_id}")
				break
	except Exception as ex:
		logger.error(f"got error on peer loop: {ex}")

	logger.info(f"close connection to {peer_id}")

	ds.remove_entity(peer_entity)


async def _update_download(env: Env, peer_entity: Entity, torrent_entity: Entity):
	ds = env.data_storage
	peer_connection_ec = peer_entity.get_component(PeerConnectionEC)
	peer_id = peer_connection_ec.connection.remote_peer_id

	# check if interested
	if not peer_connection_ec.local_interested:
		return

	info_ec = torrent_entity.get_component(TorrentInfoEC)
	info = info_ec.info

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
	piece_entity = ds.get_collection(PieceEC).find(PieceEC.make_hash(info.info_hash, index))
	if not piece_entity:
		piece_entity = ds.create_entity().add_component(PieceEC(info, index))
		piece_entity.add_component(PiecePendingRemoveEC())

	await _request_pieces(peer_connection_ec, piece_entity.get_component(PieceEC))


async def _update_interested(peer_entity: Entity, torrent_entity: Entity):
	remote_bitfield = peer_entity.get_component(BitfieldEC)
	local_bitfield = torrent_entity.get_component(BitfieldEC)
	connection = peer_entity.get_component(PeerConnectionEC)

	if local_bitfield.interested_in(remote_bitfield, exclude=set()):
		await connection.interested()
	else:
		await connection.not_interested()


async def _clear_download(env: Env, peer_entity: Entity, torrent_entity: Entity):
	ds = env.data_storage
	peer_connection_ec = peer_entity.get_component(PeerConnectionEC)
	info_hash = torrent_entity.get_component(TorrentInfoEC).info.info_hash

	pieces = peer_connection_ec.reset_progress()
	for index in pieces:
		logger.warning(f"clear download block {index, peer_connection_ec.connection.remote_peer_id}")
		piece_entity = ds.get_collection(PieceEC).find(PieceEC.make_hash(info_hash, index))
		piece_entity.get_component(PieceEC).cancel(peer_connection_ec.connection.remote_peer_id)


async def _request_pieces(peer_connection_ec: PeerConnectionEC, piece_ec: PieceEC) -> None:
	while peer_connection_ec.can_request() and piece_ec.has_next():
		index, begin, length = piece_ec.get_next(peer_connection_ec.connection.remote_peer_id)
		await peer_connection_ec.request(index, begin, length)


async def _send_have_to_peers(env: Env, info_hash: bytes, index: int):
	entities = env.data_storage.get_collection(PeerConnectionEC).entities
	for entity in entities:
		connection: Connection = entity.get_component(PeerConnectionEC).connection
		peer_ec: PeerInfoEC = entity.get_component(PeerInfoEC)
		if peer_ec.info_hash == info_hash:
			await connection.have(index)


async def _process_piece_message(env: Env, peer_entity: Entity, torrent_entity: Entity, message: Message):
	ds = env.data_storage
	info_hash = torrent_entity.get_component(TorrentInfoEC).info.info_hash
	peer_connection_ec = peer_entity.get_component(PeerConnectionEC)

	index, begin, block = message.piece

	# add block to piece
	piece_entity = ds.get_collection(PieceEC).find(PieceEC.make_hash(info_hash, index))
	piece_ec = piece_entity.get_component(PieceEC)
	piece_ec.append(begin, block)

	# remove block from connection queue
	peer_connection_ec.reset_block(index, begin)

	# whole piece was downloaded
	if piece_ec.completed:
		piece_ec.add_marker(PieceToSaveEC)
		await _send_have_to_peers(env, info_hash, index)
		logger.info(f"piece {index} completed")

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
	info = torrent_entity.get_component(TorrentInfoEC).info
	connection = peer_entity.get_component(PeerConnectionEC).connection

	index, begin, length = message.request

	piece_entity = ds.get_collection(PieceEC).find(PieceEC.make_hash(info.info_hash, index))

	# load piece
	if not piece_entity:
		root = Path(config.download_folder)
		data = load_piece(root, info, index)

		piece_entity = ds.create_entity().add_component(PieceEC(info, index, data))
		piece_entity.add_component(PiecePendingRemoveEC())

	piece_ec = piece_entity.get_component(PieceEC)
	if not piece_ec.completed:
		logger.error(f"Piece {index} in {info.name} is not completed on request")
		# TODO: how did we get here?
		return

	if not check_hash(piece_ec.data, info.pieces.get_piece_hash(index)):
		logger.error(f"Piece {index} in {info.name} torrent is broken")
		# TODO: check files, reload piece
		return

	data = piece_ec.get_block(begin, length)
	piece_entity.get_component(PiecePendingRemoveEC).update()
	torrent_entity.get_component(TorrentTrackerDataEC).update_uploaded(length)
	await connection.piece(index, begin, data)
