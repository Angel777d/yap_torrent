import asyncio
import random

from app import System, Env
from app.components.bitfield_ec import BitfieldEC
from app.components.peer_ec import PeerPendingEC, PeerInfoEC, PeerConnectionEC, PeerHandshakeEC, PeerActiveEC
from app.components.piece_ec import PieceEC, PieceToSaveEC
from app.components.torrent_ec import TorrentInfoEC
from core.DataStorage import Entity
from torrent.connection import Connection, ConnectionState, MessageId


class PeerSystem(System):

	def __init__(self, env: Env):
		super().__init__(env)
		self.__keep_alive_timeout = 10

	async def update(self, delta_time: float):
		self.remove_outdated()
		self.connect_to_new_peers()
		self.process_handshake_peers()
		self.keep_alive()

	def keep_alive(self):
		active = self.env.data_storage.get_collection(PeerActiveEC).entities
		for peer in active:
			connection = peer.get_component(PeerConnectionEC).connection
			connection.keep_alive()

	def remove_outdated(self):
		ds = self.env.data_storage
		peer_connections = ds.get_collection(PeerConnectionEC).entities
		for entity in peer_connections:
			peer_connection_ec = entity.get_component(PeerConnectionEC)
			if peer_connection_ec.connection.is_dead():
				info = entity.get_component(PeerInfoEC)
				self._clear_download(info.info_hash, peer_connection_ec)

				if peer_connection_ec.connection.state == ConnectionState.Handshake:
					if info.attempt < 20:
						info.attempt += 1
						entity.remove_component(PeerHandshakeEC)
						entity.remove_component(BitfieldEC)
						entity.remove_component(PeerConnectionEC)
						entity.add_component(PeerPendingEC())
						print(f"close for new attempt {info.attempt} for {info.peer_info.host}")
					else:
						print(f"close {info.peer_info.host} after all attempts")
				else:
					print(f"reconnect to {info.peer_info.host}")
					info_hash = entity.get_component(PeerInfoEC).info_hash
					peer_info = entity.get_component(PeerInfoEC).peer_info
					attempt = entity.get_component(PeerInfoEC).attempt
					ds.remove_entity(entity)

					ds.create_entity().add_component(PeerInfoEC(info_hash, peer_info, attempt)).add_component(
						PeerPendingEC())

		print(f"{len(ds.get_collection(PeerActiveEC))} active peers")

	def connect_to_new_peers(self):
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

			timeout = 60
			connection = Connection(timeout)

			torrent_entity = ds.get_collection(TorrentInfoEC).find(peer_ec.info_hash)
			torrent_info = torrent_entity.get_component(TorrentInfoEC).info

			peer_entity.add_component(BitfieldEC(torrent_info.pieces.num))
			peer_entity.add_component(PeerConnectionEC(connection))

			# calculate bitfield
			torrent_entity = ds.get_collection(TorrentInfoEC).find(peer_ec.info_hash)
			bitfield = torrent_entity.get_component(BitfieldEC)
			dump = bitfield.dump() if bitfield.have_num else bytes()

			handshake_task = connection.connect(peer_ec.peer_info, peer_ec.attempt, peer_ec.info_hash, my_peer_id, dump)
			peer_entity.add_component(PeerHandshakeEC(handshake_task))

	def process_handshake_peers(self):
		ds = self.env.data_storage
		handshake_peers = ds.get_collection(PeerHandshakeEC).entities

		for entity in handshake_peers:
			peer_connection_ec = entity.get_component(PeerConnectionEC)
			connection = peer_connection_ec.connection

			if connection.state == ConnectionState.Created:
				continue

			# handshake in progress. waiting for it
			if connection.state == ConnectionState.Handshake:
				continue

			# we can start do something
			if connection.state == ConnectionState.Connected:
				entity.remove_component(PeerHandshakeEC)
				entity.add_component(PeerActiveEC(asyncio.create_task(self._listen(entity))))

	def _update_interested(self, peer_entity: Entity, torrent_entity: Entity):
		remote_bitfield = peer_entity.get_component(BitfieldEC)
		local_bitfield = torrent_entity.get_component(BitfieldEC)
		connection = peer_entity.get_component(PeerConnectionEC)

		if local_bitfield.interested_in(remote_bitfield, exclude=set()):
			connection.interested()
			self._update_download(peer_entity, torrent_entity)
		else:
			connection.not_interested()

	def _clear_download(self, info_hash: bytes, peer_connection_ec: PeerConnectionEC):
		if not peer_connection_ec.download_block:
			return

		ds = self.env.data_storage
		index, begin, length = peer_connection_ec.download_block
		peer_connection_ec.download_block = None
		piece_entity = ds.get_collection(PieceEC).find(PieceEC.make_hash(info_hash, index))
		piece_entity.get_component(PieceEC).cancel(begin)

	def _update_download(self, peer_entity: Entity, torrent_entity: Entity):
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
		print("selected piece for download:", index, torrent_entity.get_component(TorrentInfoEC).info.name)

		# find or create piece
		ds = self.env.data_storage
		piece_entity = ds.get_collection(PieceEC).find(PieceEC.make_hash(info_hash, index))
		if not piece_entity:
			piece_info = info_ec.info.pieces.get_piece(index)

			piece_entity = ds.create_entity().add_component(PieceEC(info_hash, piece_info))

		self._try_load_next(peer_connection_ec, piece_entity.get_component(PieceEC))

	@staticmethod
	def _try_load_next(peer_connection_ec: PeerConnectionEC, piece_ec: PieceEC) -> bool:
		if not piece_ec.has_next():
			return False
		index, begin, length = piece_ec.get_next()
		peer_connection_ec.request(index, begin, length)
		return True

	def _send_have_to_peers(self, info_hash: bytes, index: int):
		ds = self.env.data_storage
		for e in ds.get_collection(PeerConnectionEC).entities:
			if e.get_component(PeerInfoEC).info_hash == info_hash:
				e.get_component(PeerConnectionEC).connection.have(index)

	async def _listen(self, peer_entity: Entity):
		ds = self.env.data_storage
		peer_ec = peer_entity.get_component(PeerInfoEC)
		peer_connection_ec = peer_entity.get_component(PeerConnectionEC)
		bitfield_ec = peer_entity.get_component(BitfieldEC)

		torrent_entity = ds.get_collection(TorrentInfoEC).find(peer_ec.info_hash)

		connection = peer_connection_ec.connection
		peer_id = connection.remote_peer_id
		torrent_info = torrent_entity.get_component(TorrentInfoEC).info

		while True:
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
					self._update_interested(peer_entity, torrent_entity)
					self._send_have_to_peers(peer_ec.info_hash, index)
				else:
					if not self._try_load_next(peer_connection_ec, piece_ec):
						self._update_download(peer_entity, torrent_entity)

			elif message.message_id == MessageId.REQUEST:
				index, begin, length = message.request
				# check index
				# get piece / load from disc
				# TODO: implement upload
				pass
			elif message.message_id == MessageId.HAVE:
				bitfield_ec.set_index(message.index)
				self._update_interested(peer_entity, torrent_entity)
			elif message.message_id == MessageId.CHOKE:
				peer_connection_ec.local_choked = True
				self._clear_download(peer_ec.info_hash, peer_connection_ec)
			elif message.message_id == MessageId.UNCHOKE:
				peer_connection_ec.local_choked = False
				self._update_download(peer_entity, torrent_entity)
				# TODO: fix choke algorythm
				peer_connection_ec.unchoke()
			elif message.message_id == MessageId.INTERESTED:
				# TODO: fix choke algorythm
				peer_connection_ec.remote_interested = True
				peer_connection_ec.unchoke()
			elif message.message_id == MessageId.NOT_INTERESTED:
				# TODO: fix choke algorythm
				peer_connection_ec.remote_interested = False
				peer_connection_ec.choke()
			elif message.message_id == MessageId.BITFIELD:
				bitfield_ec.update(message.bitfield)
				self._update_interested(peer_entity, torrent_entity)
