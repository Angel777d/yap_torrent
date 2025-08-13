import asyncio

from app import System, Env
from app.components.bitfield_ec import BitfieldEC
from app.components.peer_ec import PeerPendingEC, PeerInfoEC, PeerConnectionEC, PeerHandshakeEC, PeerActiveEC, \
	PeerUpdateBitfieldEC, PeerUpdateHaveEC
from app.components.piece_ec import PieceEC
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

			peer_entity.add_component(BitfieldEC(BitfieldEC.create_empty(torrent_info)))
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
		ds = self.env.data_storage
		collection = ds.get_collection(PeerUpdateBitfieldEC).entities
		for peer_entity in collection:
			peer_entity.remove_component(PeerUpdateBitfieldEC)
			await self._choose_piece(peer_entity)

		collection = ds.get_collection(PeerUpdateHaveEC).entities
		for peer_entity in collection:
			index = peer_entity.get_component(PeerUpdateHaveEC).index

			info_hash = peer_entity.get_component(PeerInfoEC).info_hash
			torrent_entity = ds.get_collection(TorrentInfoEC).find(info_hash)
			local_bitfield = torrent_entity.get_component(BitfieldEC)

			if local_bitfield.have(index):
				pass

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

	async def _choose_piece(self, peer_entity: Entity):
		if peer_entity.get_component(PieceEC):
			return

		ds = self.env.data_storage
		remote_bitfield = peer_entity.get_component(BitfieldEC)
		info_hash = peer_entity.get_component(PeerInfoEC).info_hash
		torrent_entity = ds.get_collection(TorrentInfoEC).find(info_hash)
		local_bitfield = torrent_entity.get_component(BitfieldEC)

		connection = peer_entity.get_component(PeerConnectionEC)

		if local_bitfield.is_interested_in(remote_bitfield):
			connection.interested()
			exclude = set(e.get_component(PieceEC).data.index for e in ds.get_collection(PieceEC).entities if
						  e.get_component(PieceEC).info_hash == info_hash)

			bitfields = (e.get_component(BitfieldEC) for e in ds.get_collection(PeerActiveEC).entities if
						 e.get_component(PeerInfoEC).info_hash == info_hash)

		else:
			connection.not_interested()

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
			# piece_entity = ds.get_collection(PieceEC).find(PieceEC.make_hash(peer_ec.info_hash, index))
			# piece_ec = piece_entity.get_component(PieceEC)
			# piece_ec.data.append(begin, block)
			# if piece_ec.data.completed:
			#     piece_ec.add_marker(CompletedPieceEC)

			elif message.message_id == MessageId.HAVE:
				bitfield_ec.set(message.index)
				peer_entity.add_component(PeerUpdateHaveEC(message.index))
			elif message.message_id == MessageId.CHOKE:
				pass
			elif message.message_id == MessageId.UNCHOKE:
				pass
			elif message.message_id == MessageId.INTERESTED:
				pass
			elif message.message_id == MessageId.NOT_INTERESTED:
				pass
			elif message.message_id == MessageId.BITFIELD:
				bitfield_ec.update(message.bitfield)
				bitfield_ec.add_marker(PeerUpdateBitfieldEC)
