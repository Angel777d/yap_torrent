import asyncio
import logging
import random

from app import System, Env
from app.components.bitfield_ec import BitfieldEC
from app.components.peer_ec import PeerPendingEC, PeerInfoEC, PeerConnectionEC
from app.components.piece_ec import PieceEC, PieceToSaveEC
from app.components.torrent_ec import TorrentInfoEC
from core.DataStorage import Entity
from torrent.connection import Connection, ConnectionState, MessageId

logger = logging.getLogger(__name__)


class PeerSystem(System):

	def __init__(self, env: Env):
		super().__init__(env)

	async def update(self, delta_time: float):
		# tasks = asyncio.all_tasks()
		# connections = self.env.data_storage.get_collection(PeerConnectionEC).entities
		await self.connect_to_new_peers()

	async def connect_to_new_peers(self):
		ds = self.env.data_storage

		# check capacity
		if len(ds.get_collection(PeerConnectionEC)) >= self.env.config.max_connections:
			return

		# sort and filter pending peers
		pending_peers = ds.get_collection(PeerPendingEC).entities
		# TODO: select peers to connect
		pass

		# connect to new peers
		active_collection = ds.get_collection(PeerConnectionEC)
		while len(active_collection) < self.env.config.max_connections and pending_peers:
			peer_entity = pending_peers.pop(0)
			peer_entity.remove_component(PeerPendingEC)

			peer_ec = peer_entity.get_component(PeerInfoEC)

			torrent_entity = ds.get_collection(TorrentInfoEC).find(peer_ec.info_hash)
			torrent_info = torrent_entity.get_component(TorrentInfoEC).info

			connection_timeout = 15  # TODO: move to config
			connection = Connection(connection_timeout)
			task = asyncio.create_task(self._listen(peer_entity, torrent_entity))

			peer_entity.add_component(BitfieldEC(torrent_info.pieces.num))
			peer_entity.add_component(PeerConnectionEC(connection, task))

	def close(self):
		ds = self.env.data_storage
		ds.clear_collection(PeerConnectionEC)

	async def _update_interested(self, peer_entity: Entity, torrent_entity: Entity):
		remote_bitfield = peer_entity.get_component(BitfieldEC)
		local_bitfield = torrent_entity.get_component(BitfieldEC)
		connection = peer_entity.get_component(PeerConnectionEC)

		if local_bitfield.interested_in(remote_bitfield, exclude=set()):
			await connection.interested()
			await self._update_download(peer_entity, torrent_entity)
		else:
			await connection.not_interested()

	async def _clear_download(self, info_hash: bytes, peer_connection_ec: PeerConnectionEC):
		if not peer_connection_ec.download_block:
			return

		ds = self.env.data_storage
		index, begin, length = peer_connection_ec.download_block

		logger.warning(f"clear download block {index, begin}")

		peer_connection_ec.download_block = None
		piece_entity = ds.get_collection(PieceEC).find(PieceEC.make_hash(info_hash, index))
		piece_entity.get_component(PieceEC).cancel(begin)

	async def _update_download(self, peer_entity: Entity, torrent_entity: Entity):
		peer_connection_ec = peer_entity.get_component(PeerConnectionEC)

		# check if interested
		if not peer_connection_ec.local_interested:
			return

		info_ec = torrent_entity.get_component(TorrentInfoEC)
		info_hash = info_ec.info.info_hash

		# check if choked
		if peer_connection_ec.local_choked:
			# stop wait if choked
			return

		# check download in progress
		if peer_connection_ec.download_block:
			return

		# TODO: implement strategies
		local_bitfield = torrent_entity.get_component(BitfieldEC)
		remote_bitfield = peer_entity.get_component(BitfieldEC)
		pieces = local_bitfield.interested_in(remote_bitfield, exclude=set())
		index = random.choice(list(pieces))

		logger.debug(
			f"selected piece {index} for {torrent_entity.get_component(TorrentInfoEC).info.name}. peer {peer_entity.get_component(PeerInfoEC).peer_info}")
		# find or create piece
		ds = self.env.data_storage
		piece_entity = ds.get_collection(PieceEC).find(PieceEC.make_hash(info_hash, index))
		if not piece_entity:
			piece_info = info_ec.info.pieces.get_piece(index)

			piece_entity = ds.create_entity().add_component(PieceEC(info_hash, piece_info))

		await self._try_load_next(peer_connection_ec, piece_entity.get_component(PieceEC))

	@staticmethod
	async def _try_load_next(peer_connection_ec: PeerConnectionEC, piece_ec: PieceEC) -> bool:
		if not piece_ec.has_next():
			return False
		index, begin, length = piece_ec.get_next()
		await peer_connection_ec.request(index, begin, length)
		return True

	async def _send_have_to_peers(self, info_hash: bytes, index: int):
		ds = self.env.data_storage
		entities = ds.get_collection(PeerConnectionEC).entities
		for entity in entities:
			connection: Connection = entity.get_component(PeerConnectionEC).connection
			# TODO: keep have messages for after connect
			if entity.get_component(
					PeerInfoEC).info_hash == info_hash and connection.state == ConnectionState.Connected:
				await connection.have(index)

	async def _listen(self, peer_entity: Entity, torrent_entity: Entity):
		ds = self.env.data_storage
		my_peer_id = self.env.peer_id
		peer_ec = peer_entity.get_component(PeerInfoEC)
		peer_connection_ec = peer_entity.get_component(PeerConnectionEC)
		bitfield_ec = peer_entity.get_component(BitfieldEC)

		connection = peer_connection_ec.connection
		peer_id = connection.remote_peer_id
		torrent_info = torrent_entity.get_component(TorrentInfoEC).info

		# handshake
		dump = bitfield_ec.dump()
		await connection.connect(peer_ec.peer_info, peer_ec.attempt, peer_ec.info_hash, my_peer_id, dump)

		if connection.is_dead():
			handshake_attempts = 2  # TODO: move to config
			if peer_ec.attempt < handshake_attempts:
				peer_ec.attempt += 1
				peer_entity.remove_component(BitfieldEC)
				peer_entity.remove_component(PeerConnectionEC)
				peer_entity.add_component(PeerPendingEC())
			else:
				ds.remove_entity(peer_entity)

			return

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
				await asyncio.sleep(0.2)  # for other peers
				message = await connection.read()

				if message.message_id == MessageId.PIECE:
					index, begin, block = message.piece
					# must be created at first piece request
					piece_entity = ds.get_collection(PieceEC).find(PieceEC.make_hash(peer_ec.info_hash, index))
					piece_ec = piece_entity.get_component(PieceEC)
					piece_ec.append(begin, block)
					peer_connection_ec.download_block = None
					if piece_ec.completed:
						piece_ec.add_marker(PieceToSaveEC)
						await self._update_interested(peer_entity, torrent_entity)
						await self._send_have_to_peers(peer_ec.info_hash, index)
						logger.info(f"piece {index} completed")

					else:
						result = await self._try_load_next(peer_connection_ec, piece_ec)
						if not result:
							logger.warning(f"WTF can't download full piece {index} by {peer_ec.peer_info}")
							await self._update_download(peer_entity, torrent_entity)

				elif message.message_id == MessageId.REQUEST:
					index, begin, length = message.request
					# check index
					# get piece / load from disc
					# TODO: implement upload
					pass
				elif message.message_id == MessageId.HAVE:
					bitfield_ec.set_index(message.index)
					await self._update_interested(peer_entity, torrent_entity)
				elif message.message_id == MessageId.CHOKE:
					peer_connection_ec.local_choked = True
					await self._clear_download(peer_ec.info_hash, peer_connection_ec)
				elif message.message_id == MessageId.UNCHOKE:
					peer_connection_ec.local_choked = False
					await self._update_download(peer_entity, torrent_entity)
					# TODO: fix choke algorythm
					await peer_connection_ec.unchoke()
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
					await self._update_interested(peer_entity, torrent_entity)
				elif message.message_id == MessageId.ERROR:
					logger.info(f"got message error on peer {peer_ec.peer_info.host}")
					break
		except Exception as ex:
			logger.error(f"got error on peer loop: {ex}")

		# keep_alive_task.cancel()
		# keep_alive_task = None
		logger.info(f"close connection to {peer_ec.peer_info.host}")
		# ds.remove_entity(peer_entity)
		peer_entity.remove_component(BitfieldEC)
		peer_entity.remove_component(PeerConnectionEC)
		peer_entity.add_component(PeerPendingEC())
