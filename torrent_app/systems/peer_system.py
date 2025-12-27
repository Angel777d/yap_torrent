import asyncio
import logging
from asyncio import StreamReader, StreamWriter
from functools import partial
from typing import Iterable

import torrent_app.protocol.connection as net
from angelovichcore.DataStorage import Entity
from torrent_app import System, Env
from torrent_app.components.bitfield_ec import BitfieldEC
from torrent_app.components.peer_ec import PeerPendingEC, PeerInfoEC, PeerConnectionEC, KnownPeersEC, \
	KnownPeersUpdateEC, PeerDisconnectedEC
from torrent_app.components.torrent_ec import TorrentInfoEC, TorrentHashEC, ValidateTorrentEC
from torrent_app.protocol import extensions
from torrent_app.protocol.bt_main_messages import bitfield
from torrent_app.protocol.extensions import create_reserved, merge_reserved
from torrent_app.protocol.structures import PeerInfo

logger = logging.getLogger(__name__)

# TODO: build dynamically from systems
LOCAL_RESERVED = create_reserved(extensions.DHT, extensions.EXTENSION_PROTOCOL)


class PeerSystem(System):

	def __init__(self, env: Env):
		super().__init__(env)

	def close(self):
		ds = self.env.data_storage
		ds.clear_collection(PeerConnectionEC)

		self.env.event_bus.remove_all_listeners(scope=self)
		super().close()

	async def start(self):
		self.env.event_bus.add_listener("peers.update", self._on_peers_update, scope=self)

		port = self.env.config.port
		host = self.env.ip
		await asyncio.start_server(self._server_callback, host, port)

	async def _update(self, delta_time: float):
		ds = self.env.data_storage

		# cleanup disconnected peers:
		remove_collection = ds.get_collection(PeerDisconnectedEC).entities
		for entity in remove_collection:
			ds.remove_entity(entity)

		# update new peers first
		update_collection = ds.get_collection(KnownPeersUpdateEC)
		for torrent_entity in update_collection.entities:
			torrent_entity.remove_component(KnownPeersUpdateEC)
			info_hash = torrent_entity.get_component(TorrentHashEC).info_hash
			for peer in torrent_entity.get_component(KnownPeersEC).peers:
				if not ds.get_collection(PeerInfoEC).find(PeerInfoEC.make_hash(info_hash, peer)):
					ds.create_entity().add_component(PeerInfoEC(info_hash, peer)).add_component(PeerPendingEC())

		active_collection = ds.get_collection(PeerConnectionEC)

		def is_capacity_full():
			return len(active_collection) >= self.env.config.max_connections

		# check capacity first
		if is_capacity_full():
			return

		# sort and filter pending peers
		# TODO: select peers to connect
		pending_peers = ds.get_collection(PeerPendingEC).entities

		my_peer_id = self.env.peer_id

		# connect to new peers
		for peer_entity in pending_peers:
			if is_capacity_full():
				break

			if ds.get_collection(TorrentHashEC).find(peer_entity.get_component(PeerInfoEC).info_hash).has_component(
					ValidateTorrentEC):
				continue

			peer_entity.remove_component(PeerPendingEC)
			self.add_task(self._connect(peer_entity, my_peer_id))

	async def _on_peers_update(self, info_hash: bytes, peers: Iterable[PeerInfo]):
		ds = self.env.data_storage

		torrent_entity = ds.get_collection(TorrentHashEC).find(info_hash)
		if not torrent_entity:
			return

		torrent_entity.get_component(KnownPeersEC).update_peers(peers)
		torrent_entity.add_component(KnownPeersUpdateEC())

	async def _server_callback(self, reader: StreamReader, writer: StreamWriter):
		logger.debug('some peer connected to us')
		local_peer_id = self.env.peer_id

		result = await net.on_connect(local_peer_id, reader, writer, LOCAL_RESERVED)
		if result is None:
			return

		pstrlen, pstr, remote_reserved, info_hash, remote_peer_id = result

		# get peer info from
		host, port = writer.transport.get_extra_info('peername')
		peer_entity = self.env.data_storage.create_entity().add_component(PeerInfoEC(info_hash, PeerInfo(host, port)))

		logger.debug(f'peer {remote_peer_id} is connected to us')

		torrent_entity = self.env.data_storage.get_collection(TorrentHashEC).find(info_hash)
		if torrent_entity:
			reserved = merge_reserved(LOCAL_RESERVED, remote_reserved)

			await self._add_peer(peer_entity, remote_peer_id, reader, writer, reserved)
		else:
			# TODO: handle no torrent for info hash
			# logger.warning(f"no torrent for info hash [{info_hash}]. handshake: {result}")
			writer.close()
			pass

	async def _connect(self, peer_entity: Entity, my_peer_id: bytes):
		peer_ec: PeerInfoEC = peer_entity.get_component(PeerInfoEC)
		result = await net.connect(peer_ec.peer_info, peer_ec.info_hash, my_peer_id, reserved=LOCAL_RESERVED)
		if not result:
			return
		remote_peer_id, reader, writer, remote_reserved = result
		reserved = merge_reserved(LOCAL_RESERVED, remote_reserved)

		await self._add_peer(peer_entity, remote_peer_id, reader, writer, reserved)

	async def _add_peer(self, peer_entity: Entity, remote_peer_id: bytes, reader: StreamReader, writer: StreamWriter,
	                    reserved: bytes) -> None:

		ds = self.env.data_storage
		info_hash = peer_entity.get_component(PeerInfoEC).info_hash
		torrent_entity = ds.get_collection(TorrentHashEC).find(info_hash)

		peer_entity.add_component(BitfieldEC())

		connection = net.Connection(remote_peer_id, reader, writer)

		# send bitfield first
		local_bitfield = torrent_entity.get_component(BitfieldEC)
		if local_bitfield.have_num > 0:
			torrent_info_ec = torrent_entity.get_component(TorrentInfoEC)
			await connection.send(bitfield(local_bitfield.dump(torrent_info_ec.info.pieces.num)))

		connection_ec = PeerConnectionEC(connection, reserved)
		peer_entity.add_component(connection_ec)

		# notify systems about a new peer
		# wait for it before start listening to messages
		await asyncio.gather(
			*self.env.event_bus.dispatch("peer.connected", torrent_entity, peer_entity)
		)

		# start listening to messages
		connection_ec.task = asyncio.create_task(_read_messages(self.env, torrent_entity, peer_entity))


async def _read_messages(env, torrent_entity: Entity, peer_entity: Entity):
	connection = peer_entity.get_component(PeerConnectionEC).connection

	message_callback = partial(env.event_bus.dispatch, "peer.message", torrent_entity, peer_entity)
	# main peer loop
	while True:
		if connection.is_dead():
			logger.info(f"Peer diconnected by other side or timeout")
			break

		# read the next message. break if no message
		if not await connection.read(message_callback):
			break

		# no message and no error - means keep alive.
		# let's sleep here for a while
		await asyncio.sleep(1)

	peer_entity.get_component(PeerConnectionEC).disconnect()
	peer_entity.add_component(PeerDisconnectedEC())
