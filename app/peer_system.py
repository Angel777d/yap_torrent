import asyncio
import random

from app import System, Env
from app.components.bitfield_ec import BitfieldEC
from app.components.peer_ec import PeerPendingEC, PeerInfoEC, PeerConnectionEC, PeerHandshakeEC, PeerActiveEC, \
	PeerDownloadEC
from app.components.piece_ec import PieceEC, CompletedPieceEC
from app.components.torrent_ec import TorrentInfoEC
from core.DataStorage import Entity
from torrent.connection import Connection, ConnectionState, MessageId


class PeerSystem(System):

	def __init__(self, env: Env):
		super().__init__(env)

	async def update(self, delta_time: float):
		await self.remove_outdated()
		await self.connect_to_new_peers()
		await self.process_handshake_peers()
		await self.process_active_peers()

	async def remove_outdated(self):
		ds = self.env.data_storage
		peer_connections = ds.get_collection(PeerConnectionEC).entities
		for entity in peer_connections:
			if entity.get_component(PeerConnectionEC).connection.is_dead():
				ds.remove_entity(entity)

	async def connect_to_new_peers(self):
		ds = self.env.data_storage

		# check capacity
		if len(ds.get_collection(PeerConnectionEC)) >= self.env.config.max_connections:
			return

		# sort and filter pending peers
		pending_peers = ds.get_collection(PeerPendingEC).entities
		# TODO: implement
		pass

		# connect to new peers
		my_peer_id = self.env.peer_id
		active_collection = ds.get_collection(PeerConnectionEC)
		while len(active_collection) < self.env.config.max_connections and pending_peers:
			peer_entity = pending_peers.pop(0)
			peer_entity.remove_component(PeerPendingEC)

			peer_ec = peer_entity.get_component(PeerInfoEC)

			connection = Connection()

			torrent_entity = ds.get_collection(TorrentInfoEC).find(peer_ec.info_hash)
			torrent_info = torrent_entity.get_component(TorrentInfoEC).info

			peer_entity.add_component(BitfieldEC(torrent_info.pieces.num))
			peer_entity.add_component(PeerConnectionEC(connection))
			peer_entity.add_component(
				PeerHandshakeEC(connection.connect(peer_ec.peer_info, peer_ec.info_hash, my_peer_id)))

	async def process_handshake_peers(self):
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

	async def process_active_peers(self):
		# wait for bitfield or have. mark bitfield update
		# mark peer as have parts
		# send interested
		# wait unchoke
		# choose piece to download
		# ask for block, repeat
		# piece complete. save it, send have to all (potential interested???) related peers
		# no more pieces. send not interested to peer

		# PeerInfoEC - has peer info: address and port
		# PeerPendingEC - tmp. waiting in queue
		# PeerConnectionEC - start connection
		# PeerHandshakeEC - tmp. handshake in progress
		# PeerActiveEC - connection and handshake done
		# PeerUpdateBitfieldEC
		pass

	async def _update_interested(self, peer_entity: Entity, torrent_entity: Entity):
		if peer_entity.get_component(PieceEC):
			return

		# ds = self.env.data_storage
		remote_bitfield = peer_entity.get_component(BitfieldEC)
		# info_hash = peer_entity.get_component(PeerInfoEC).info_hash
		# torrent_entity = ds.get_collection(TorrentInfoEC).find(info_hash)
		local_bitfield = torrent_entity.get_component(BitfieldEC)

		connection = peer_entity.get_component(PeerConnectionEC)

		# exclude = set(e.get_component(PieceEC).index for e in ds.get_collection(PieceEC).entities if
		# 			  e.get_component(PieceEC).info_hash == info_hash)
		if local_bitfield.interested_in(remote_bitfield, exclude=set()):
			connection.interested()
		else:
			connection.not_interested()

	async def _update_download(self, peer_entity: Entity, torrent_entity: Entity):
		peer_connection_ec = peer_entity.get_component(PeerConnectionEC)

		# check we already download something
		if peer_entity.get_component(PeerDownloadEC):
			return

		local_bitfield = torrent_entity.get_component(BitfieldEC)
		remote_bitfield = peer_entity.get_component(BitfieldEC)

		# TODO: implement strategies
		pieces = local_bitfield.interested_in(remote_bitfield, exclude=set())
		index = random.choice(list(pieces))

		# find or create piece
		ds = self.env.data_storage
		peer_ec = peer_entity.get_component(PeerInfoEC)
		piece_entity = ds.get_collection(PieceEC).find(PieceEC.make_hash(peer_ec.info_hash, index))
		info_ec = torrent_entity.get_component(TorrentInfoEC)
		if not piece_entity:
			piece_entity = ds.create_entity().add_component(
				PieceEC(peer_ec.info_hash, index, info_ec.info.pieces.piece_length))

		peer_entity.add_component(PeerDownloadEC(piece_entity.entity_id))
		await self._load_next(peer_entity, piece_entity)

	async def _load_next(self, peer_entity: Entity, piece_entity: Entity):
		piece = piece_entity.get_component(PieceEC)
		index = piece.index
		begin = piece.get_next_begin()
		length = piece.block_size
		peer_entity.get_component(PeerConnectionEC).connection.request(index, begin, length)

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
				if piece_ec.completed:
					piece_ec.add_marker(CompletedPieceEC)
					peer_entity.remove_component(PeerDownloadEC)
					torrent_entity.get_component(BitfieldEC).set_index(piece_ec.index)
					await self._update_interested(peer_entity, torrent_entity)
					await self._update_download(peer_entity, torrent_entity)
				else:
					await self._load_next(peer_entity, piece_entity)

			elif message.message_id == MessageId.HAVE:
				bitfield_ec.set_index(message.index)
				await self._update_interested(peer_entity, torrent_entity)
				await self._update_download(peer_entity, torrent_entity)
			elif message.message_id == MessageId.CHOKE:
				peer_connection_ec.local_unchoked = False
				await self._update_download(peer_entity, torrent_entity)
			elif message.message_id == MessageId.UNCHOKE:
				peer_connection_ec.local_unchoked = True
				await self._update_download(peer_entity, torrent_entity)
			elif message.message_id == MessageId.INTERESTED:
				# TODO: fix choke algorythm
				peer_connection_ec.remove_interested = True
				peer_connection_ec.unchoke()
			elif message.message_id == MessageId.NOT_INTERESTED:
				# TODO: fix choke algorythm
				peer_connection_ec.remove_interested = False
				peer_connection_ec.choke()
			elif message.message_id == MessageId.BITFIELD:
				bitfield_ec.update(message.bitfield)
				await self._update_interested(peer_entity, torrent_entity)
				await self._update_download(peer_entity, torrent_entity)
