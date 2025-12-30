import asyncio
import logging
import time
from asyncio import StreamReader, StreamWriter
from dataclasses import dataclass
from functools import partial
from typing import Iterable, Set, Dict

import torrent_app.protocol.connection as net
from angelovichcore.DataStorage import Entity
from torrent_app import System, Env
from torrent_app.components.bitfield_ec import BitfieldEC
from torrent_app.components.peer_ec import PeerInfoEC, PeerConnectionEC, KnownPeersEC, PeerDisconnectedEC
from torrent_app.components.torrent_ec import TorrentInfoEC, TorrentHashEC, ValidateTorrentEC
from torrent_app.protocol import extensions
from torrent_app.protocol.bt_main_messages import bitfield
from torrent_app.protocol.extensions import create_reserved, merge_reserved
from torrent_app.protocol.structures import PeerInfo

logger = logging.getLogger(__name__)

# TODO: build dynamically from systems
LOCAL_RESERVED = create_reserved(extensions.DHT, extensions.EXTENSION_PROTOCOL)

FAIL_COOLDOWN = 60
MAX_FAILS_COUNT = 3


@dataclass
class HostInfo:
	peer: PeerInfo
	torrents: Set[bytes]
	last_fail_time: float
	fails_count: int = 0
	in_use: bool = False

	def fail(self):
		self.fails_count += 1
		self.last_fail_time = time.monotonic()

	def on_cooldown(self) -> bool:
		return self.fails_count and time.monotonic() - self.last_fail_time < FAIL_COOLDOWN


class PeersManager:
	def __init__(self):
		self.banned: Set[str] = set()
		self.hosts: Dict[str, HostInfo] = dict()

	def update(self, info_hash: bytes, peers: Iterable[PeerInfo]):
		for peer in peers:
			if peer.host in self.banned:
				continue
			self.hosts.setdefault(peer.host, HostInfo(peer, set(), .0)).torrents.add(info_hash)

	def mark_failed(self, peer: PeerInfo):
		host = self.hosts.get(peer.host)
		host.fail()

	def free(self, peer: PeerInfo):
		host = self.hosts.get(peer.host)
		if not host:
			return
		if host.fails_count > MAX_FAILS_COUNT:
			self.banned.add(peer.host)
			del self.hosts[peer.host]
		else:
			host.in_use = False

	def use(self, peer: PeerInfo):
		host = self.hosts.get(peer.host)
		host.in_use = True

	def get_hosts(self) -> Iterable[HostInfo]:
		return sorted(
			(host for host in self.hosts.values() if not (host.on_cooldown() or host.in_use)),
			key=lambda h: h.fails_count
		)


class PeerSystem(System):

	def __init__(self, env: Env):
		super().__init__(env)
		self.manager = PeersManager()

	async def start(self):
		port = self.env.config.port
		host = self.env.ip
		await asyncio.start_server(self._server_callback, host, port)

		self.env.event_bus.add_listener("peers.update", self._on_peers_update, scope=self)

		for torrent_entity in self.env.data_storage.get_collection(KnownPeersEC).entities:
			info_hash = torrent_entity.get_component(TorrentHashEC).info_hash
			peers = torrent_entity.get_component(KnownPeersEC).peers
			self.manager.update(info_hash, peers)

	def close(self):
		ds = self.env.data_storage
		ds.clear_collection(PeerInfoEC)

		self.env.event_bus.remove_all_listeners(scope=self)
		super().close()

	async def _update(self, delta_time: float):
		ds = self.env.data_storage

		# cleanup disconnected peers:
		to_remove = ds.get_collection(PeerDisconnectedEC).entities
		for peer_entity in to_remove:
			self.manager.free(peer_entity.get_component(PeerInfoEC).peer_info)
			ds.remove_entity(peer_entity)

		active_collection = ds.get_collection(PeerInfoEC)

		def is_capacity_full():
			return len(active_collection) >= self.env.config.max_connections

		# check capacity first
		if is_capacity_full():
			return

		my_peer_id = self.env.peer_id

		# sort and filter pending peers
		suitable_hosts = self.manager.get_hosts()
		for host in suitable_hosts:
			if is_capacity_full():
				break

			for info_hash in host.torrents:
				if ds.get_collection(TorrentHashEC).find(info_hash).has_component(ValidateTorrentEC):
					continue

				self.manager.use(host.peer)
				peer_entity = ds.create_entity().add_component(PeerInfoEC(info_hash, host.peer))
				self.add_task(self._connect(peer_entity, my_peer_id))

	async def _on_peers_update(self, info_hash: bytes, peers: Iterable[PeerInfo]):
		ds = self.env.data_storage

		torrent_entity = ds.get_collection(TorrentHashEC).find(info_hash)
		if not torrent_entity:
			return

		torrent_entity.get_component(KnownPeersEC).update_peers(peers)
		self.manager.update(info_hash, peers)

	async def _server_callback(self, reader: StreamReader, writer: StreamWriter):
		logger.debug('some peer connected to us')
		local_peer_id = self.env.peer_id

		result = await net.on_connect(local_peer_id, reader, writer, LOCAL_RESERVED)
		if result is None:
			return

		pstrlen, pstr, remote_reserved, info_hash, remote_peer_id = result

		# get peer info from
		ds = self.env.data_storage
		host, port = writer.transport.get_extra_info('peername')

		logger.debug(f'peer {remote_peer_id} is connected to us')

		torrent_entity = ds.get_collection(TorrentHashEC).find(info_hash)
		if torrent_entity:
			reserved = merge_reserved(LOCAL_RESERVED, remote_reserved)
			peer_entity = ds.create_entity().add_component(PeerInfoEC(info_hash, PeerInfo(host, port)))

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
			peer_entity.add_component(PeerDisconnectedEC())
			self.manager.mark_failed(peer_ec.peer_info)
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
			await connection.send(bitfield(local_bitfield.dump(torrent_info_ec.info.pieces_num)))

		connection_ec = PeerConnectionEC(connection, reserved)
		peer_entity.add_component(connection_ec)

		# notify systems about a new peer
		# wait for it before start listening to messages
		await asyncio.gather(
			*self.env.event_bus.dispatch("peer.connected", torrent_entity, peer_entity)
		)

		# start listening to messages
		connection_ec.task = asyncio.create_task(self._read_messages(torrent_entity, peer_entity))

	async def _read_messages(self, torrent_entity: Entity, peer_entity: Entity):
		connection = peer_entity.get_component(PeerConnectionEC).connection

		message_callback = partial(self.env.event_bus.dispatch, "peer.message", torrent_entity, peer_entity)
		# main peer loop
		while True:
			if connection.is_dead():
				logger.debug("Peer diconnected by other side or timeout")
				break

			# read the next message. return False in case of error
			if not await connection.read(message_callback):
				peer_info = peer_entity.get_component(PeerInfoEC).peer_info
				self.manager.mark_failed(peer_info)
				break

		peer_entity.get_component(PeerConnectionEC).disconnect()
		peer_entity.add_component(PeerDisconnectedEC())
