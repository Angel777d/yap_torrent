import asyncio
import logging
import random
from asyncio import StreamReader, StreamWriter
from pathlib import Path

from app import System, Env, Config
from app.components.bitfield_ec import BitfieldEC
from app.components.peer_ec import PeerPendingEC, PeerInfoEC, PeerConnectionEC
from app.components.piece_ec import PieceEC, PieceToSaveEC, PiecePendingRemoveEC
from app.components.torrent_ec import TorrentInfoEC
from app.utils import load_piece
from core.DataStorage import Entity, DataStorage
from torrent import TorrentInfo
from torrent.connection import Connection, MessageId, connect, on_connect, Message

logger = logging.getLogger(__name__)


async def _update_download(ds: DataStorage, peer_entity: Entity, torrent_entity: Entity):
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
		peer_info = None
		# peer_name = writer.transport.get_extra_info('peername')
		# if peer_name is not None:
		# 	host, port = peer_name
		# 	peer_info = PeerInfo(host, port)
		# 	print("connections from", host, port)

		peer_entity = self.env.data_storage.create_entity().add_component(PeerInfoEC(info_hash, peer_info))

		logger.info(f'peer {remote_peer_id} is connected to us')

		torrent_entity = self.env.data_storage.get_collection(TorrentInfoEC).find(info_hash)
		if torrent_entity:
			await self._add_peer(peer_entity, info_hash, remote_peer_id, reader, writer)
		else:
			logger.error(f"no torrent for info hash{info_hash}")

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
			peer_ec = peer_entity.get_component(PeerInfoEC)

			result = await connect(peer_ec.peer_info, peer_ec.info_hash, my_peer_id)
			if not result:
				continue
			remote_peer_id, reader, writer = result

			await self._add_peer(peer_entity, peer_ec.info_hash, remote_peer_id, reader, writer)

	async def _add_peer(self, peer_entity: Entity, info_hash: bytes, remote_peer_id: bytes, reader: StreamReader,
	                    writer: StreamWriter):
		ds = self.env.data_storage

		torrent_entity = ds.get_collection(TorrentInfoEC).find(info_hash)
		info = torrent_entity.get_component(TorrentInfoEC).info

		connection = Connection(remote_peer_id, reader, writer)

		loop = asyncio.get_running_loop()
		task = asyncio.create_task(await loop.run_in_executor(None, self._listen, peer_entity, torrent_entity))

		peer_entity.add_component(BitfieldEC(info.pieces.num))
		peer_entity.add_component(PeerConnectionEC(connection, task))

	async def _listen(self, peer_entity: Entity, torrent_entity: Entity):
		ds = self.env.data_storage
		config = self.env.config
		peer_ec = peer_entity.get_component(PeerInfoEC)
		peer_connection_ec = peer_entity.get_component(PeerConnectionEC)
		bitfield_ec = peer_entity.get_component(BitfieldEC)

		connection = peer_connection_ec.connection
		peer_id = connection.remote_peer_id
		info = torrent_entity.get_component(TorrentInfoEC).info

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
					await _process_piece_message(ds, message, peer_entity, torrent_entity)
				elif message.message_id == MessageId.REQUEST:
					await _process_request_message(ds, message, connection, info, config)
				elif message.message_id == MessageId.HAVE:
					bitfield_ec.set_index(message.index)
					await _update_interested(peer_entity, torrent_entity)
					if peer_connection_ec.is_free_to_download():
						await _update_download(ds, peer_entity, torrent_entity)
				elif message.message_id == MessageId.CHOKE:
					peer_connection_ec.local_choked = True
					await _clear_download(ds, peer_ec.info_hash, peer_connection_ec)
				elif message.message_id == MessageId.UNCHOKE:
					if peer_connection_ec.local_choked:
						peer_connection_ec.local_choked = False
						await _update_download(ds, peer_entity, torrent_entity)
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
					await _update_download(ds, peer_entity, torrent_entity)
				elif message.message_id == MessageId.ERROR:
					logger.debug(f"got message error on peer {peer_id}")
					break
		except Exception as ex:
			logger.error(f"got error on peer loop: {ex}")

		logger.info(f"close connection to {peer_id}")

		ds.remove_entity(peer_entity)


async def _update_interested(peer_entity: Entity, torrent_entity: Entity):
	remote_bitfield = peer_entity.get_component(BitfieldEC)
	local_bitfield = torrent_entity.get_component(BitfieldEC)
	connection = peer_entity.get_component(PeerConnectionEC)

	if local_bitfield.interested_in(remote_bitfield, exclude=set()):
		await connection.interested()
	else:
		await connection.not_interested()


async def _clear_download(ds: DataStorage, info_hash: bytes, peer_connection_ec: PeerConnectionEC):
	pieces = peer_connection_ec.reset_progress()
	for index in pieces:
		logger.warning(f"clear download block {index, peer_connection_ec.connection.remote_peer_id}")
		piece_entity = ds.get_collection(PieceEC).find(PieceEC.make_hash(info_hash, index))
		piece_entity.get_component(PieceEC).cancel(peer_connection_ec.connection.remote_peer_id)


async def _request_pieces(peer_connection_ec: PeerConnectionEC, piece_ec: PieceEC) -> None:
	while peer_connection_ec.can_request() and piece_ec.has_next():
		index, begin, length = piece_ec.get_next(peer_connection_ec.connection.remote_peer_id)
		await peer_connection_ec.request(index, begin, length)


async def _send_have_to_peers(ds: DataStorage, info_hash: bytes, index: int):
	entities = ds.get_collection(PeerConnectionEC).entities
	for entity in entities:
		connection: Connection = entity.get_component(PeerConnectionEC).connection
		if entity.get_component(PeerInfoEC).info_hash == info_hash:
			await connection.have(index)


async def _process_piece_message(ds: DataStorage, message: Message, peer_entity: Entity, torrent_entity: Entity):
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
		await _send_have_to_peers(ds, info_hash, index)
		logger.info(f"piece {index} completed")

		await _update_interested(peer_entity, torrent_entity)

	# request for next pieces
	else:
		await _request_pieces(peer_connection_ec, piece_ec)

	# go to next piece
	if peer_connection_ec.is_free_to_download():
		await _update_download(ds, peer_entity, torrent_entity)


async def _process_request_message(
		ds: DataStorage,
		message: Message,
		connection: Connection,
		info: TorrentInfo,
		config: Config
):
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
		logger.error(f"Piece {index} in {info.name} is not completed")
		# TODO: how did we get here?
		return

	if not PieceEC.check_hash(piece_ec.data, info.pieces.get_piece_hash(index)):
		logger.error(f"Piece {index} in {info.name} torrent is broken")
		# TODO: check files, reload piece
		return

	data = piece_ec.get_block(begin, length)
	piece_entity.get_component(PiecePendingRemoveEC).update()
	await connection.piece(index, begin, data)
