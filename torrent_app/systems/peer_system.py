import asyncio
import logging
from asyncio import StreamReader, StreamWriter

from angelovichcore.DataStorage import Entity
from torrent_app import System, Env
from torrent_app.components.bitfield_ec import BitfieldEC
from torrent_app.components.peer_ec import PeerPendingEC, PeerInfoEC, PeerConnectionEC
from torrent_app.components.torrent_ec import TorrentInfoEC, TorrentHashEC
from torrent_app.protocol import extensions
from torrent_app.protocol.bt_main_messages import bitfield
from torrent_app.protocol.connection import Connection, connect, on_connect
from torrent_app.protocol.extensions import create_reserved, merge_reserved
from torrent_app.protocol.structures import PeerInfo

logger = logging.getLogger(__name__)

# TODO: move to config?
LOCAL_RESERVED = create_reserved(extensions.DHT, extensions.EXTENSION_PROTOCOL)


class PeerSystem(System):

	def __init__(self, env: Env):
		super().__init__(env)

	def close(self):
		ds = self.env.data_storage
		ds.clear_collection(PeerConnectionEC)
		super().close()

	async def start(self):
		port = self.env.config.port
		host = self.env.ip
		await asyncio.start_server(self._server_callback, host, port)

	async def _server_callback(self, reader: StreamReader, writer: StreamWriter):
		logger.debug('some peer connected to us')
		local_peer_id = self.env.peer_id

		result = await on_connect(local_peer_id, reader, writer, LOCAL_RESERVED)
		if result is None:
			return

		pstrlen, pstr, remote_reserved, info_hash, remote_peer_id = result

		# get peer info from
		host, port = writer.transport.get_extra_info('peername')
		peer_entity = self.env.data_storage.create_entity().add_component(PeerInfoEC(info_hash, PeerInfo(host, 0)))

		logger.info(f'peer {remote_peer_id} is connected to us')

		torrent_entity = self.env.data_storage.get_collection(TorrentHashEC).find(info_hash)
		if torrent_entity:
			reserved = merge_reserved(LOCAL_RESERVED, remote_reserved)

			await self._add_peer(peer_entity, remote_peer_id, reader, writer, reserved)
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
		result = await connect(peer_ec.peer_info, peer_ec.info_hash, my_peer_id, reserved=LOCAL_RESERVED)
		if not result:
			return
		remote_peer_id, reader, writer, remote_reserved = result
		reserved = merge_reserved(LOCAL_RESERVED, remote_reserved)

		await self._add_peer(peer_entity, remote_peer_id, reader, writer, reserved)

	async def _add_peer(self, peer_entity: Entity, remote_peer_id: bytes, reader: StreamReader,
	                    writer: StreamWriter, reserved: bytes) -> None:
		connection = Connection(remote_peer_id, reader, writer)

		task = asyncio.create_task(_listen(self.env, peer_entity))

		peer_entity.add_component(BitfieldEC())
		peer_entity.add_component(PeerConnectionEC(connection, task, reserved))


async def _listen(env: Env, peer_entity: Entity) -> None:
	ds = env.data_storage
	connection = peer_entity.get_component(PeerConnectionEC).connection
	peer_id = connection.remote_peer_id

	info_hash = peer_entity.get_component(PeerInfoEC).info_hash
	torrent_entity = ds.get_collection(TorrentHashEC).find(info_hash)

	# send bitfield first
	local_bitfield = torrent_entity.get_component(BitfieldEC)
	if local_bitfield.have_num > 0:
		torrent_info_ec = torrent_entity.get_component(TorrentInfoEC)
		await connection.send(bitfield(local_bitfield.dump(torrent_info_ec.info.pieces.num)))

	# notify systems about new peer
	await env.event_bus.dispatch("peer.connected", torrent_entity, peer_entity)

	# main peer loop
	try:
		while not connection.is_dead():
			# read next message
			message, error = await connection.read()

			# in case of error just log and exit
			if error:
				logger.error(error)
				break
			# process messages
			elif message:
				await env.event_bus.dispatch("peer.message", torrent_entity, peer_entity, message)
			# means keep alive. no message and no error
			else:
				pass
	except Exception as ex:
		logger.error(f"got error on peer loop: {ex}")

	logger.info(f"close connection to {peer_id}")
	ds.remove_entity(peer_entity)
