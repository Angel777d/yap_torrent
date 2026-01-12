import asyncio
import logging
import time
from asyncio import StreamReader, StreamWriter, Server
from dataclasses import dataclass
from typing import Iterable, Set, Dict, Iterator

from angelovich.core.DataStorage import Entity

import yap_torrent.protocol.connection as net
from yap_torrent.components.bitfield_ec import BitfieldEC
from yap_torrent.components.peer_ec import PeerInfoEC, PeerConnectionEC, KnownPeersEC, PeerDisconnectedEC
from yap_torrent.components.torrent_ec import TorrentInfoEC, TorrentHashEC, ValidateTorrentEC
from yap_torrent.env import Env
from yap_torrent.protocol import extensions
from yap_torrent.protocol.bt_main_messages import bitfield
from yap_torrent.protocol.extensions import create_reserved, merge_reserved
from yap_torrent.protocol.structures import PeerInfo
from yap_torrent.system import System
from yap_torrent.systems import is_torrent_complete, iterate_peers

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

	def mark_good(self, peer: PeerInfo):
		self.hosts.get(peer.host).fails_count = 0

	def mark_failed(self, peer: PeerInfo):
		self.hosts.get(peer.host).fail()

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

	def remove_torrent(self, info_hash: bytes):
		for host in self.hosts.values():
			host.torrents.discard(info_hash)

class PeerSystem(System):

	def __init__(self, env: Env):
		super().__init__(env)
		self.manager = PeersManager()
		self.server: Server = None

	async def start(self):
		port = self.env.config.port
		host = self.env.ip
		self.server = await asyncio.start_server(self._server_callback, host, port)

		self.env.event_bus.add_listener("peers.update", self._on_peers_update, scope=self)
		self.env.event_bus.add_listener("action.torrent.complete", self._on_torrent_complete, scope=self)
		self.env.event_bus.add_listener("request.torrent.invalidate", self._on_torrent_stop, scope=self)
		self.env.event_bus.add_listener("action.torrent.remove", self._on_torrent_stop, scope=self)
		self.env.event_bus.add_listener("action.torrent.stop", self._on_torrent_stop, scope=self)

		for torrent_entity in self.env.data_storage.get_collection(KnownPeersEC).entities:
			info_hash = torrent_entity.get_component(TorrentHashEC).info_hash
			peers = torrent_entity.get_component(KnownPeersEC).peers
			self.manager.update(info_hash, peers)

	def close(self):
		self.server.close()

		ds = self.env.data_storage
		ds.clear_collection(PeerInfoEC)

		self.env.event_bus.remove_all_listeners(scope=self)
		super().close()

	async def _update(self, delta_time: float):
		ds = self.env.data_storage

		# cleanup disconnected peers:
		to_remove = ds.get_collection(PeerDisconnectedEC).entities
		for peer_entity in to_remove:
			peer_entity.get_component(PeerConnectionEC).disconnect()
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
				torrent_entity = ds.get_collection(TorrentHashEC).find(info_hash)
				if not torrent_entity:
					logger.error(f"Torrent not found for host {host}")
					continue
				# skip completed torrents
				if is_torrent_complete(torrent_entity):
					continue
				# skip torrents in validation
				if torrent_entity.has_component(ValidateTorrentEC):
					continue

				self.manager.use(host.peer)
				self.add_task(self._connect(my_peer_id, info_hash, host.peer))

	async def _on_torrent_complete(self, torrent_entity: Entity):
		info_hash = torrent_entity.get_component(TorrentHashEC).info_hash
		_disconnect_peers(
			p for p in iterate_peers(self.env, info_hash) if
			p.get_component(PeerConnectionEC).remote_interested
		)

	async def _on_torrent_stop(self, info_hash: bytes):
		_disconnect_peers(p for p in iterate_peers(self.env, info_hash))
		self.manager.remove_torrent(info_hash)

	async def _on_peers_update(self, info_hash: bytes, peers: Iterable[PeerInfo]):
		ds = self.env.data_storage

		torrent_entity = ds.get_collection(TorrentHashEC).find(info_hash)
		if not torrent_entity:
			return

		torrent_entity.get_component(KnownPeersEC).update_peers(peers)
		self.manager.update(info_hash, peers)

	async def _server_callback(self, reader: StreamReader, writer: StreamWriter):
		peer_info = PeerInfo(*writer.transport.get_extra_info('peername'))
		logger.info('%s connected to us', peer_info)

		# parse handshake
		local_peer_id = self.env.peer_id
		result = await net.on_connect(local_peer_id, reader, writer, LOCAL_RESERVED)
		if result is None:
			return

		# unpack handshake
		pstrlen, pstr, remote_reserved, info_hash, remote_peer_id = result

		# get peer info from
		torrent_entity = self.env.data_storage.get_collection(TorrentHashEC).find(info_hash)
		if not torrent_entity:
			logger.debug("%s asks for torrent %s we don't have", peer_info, info_hash)
			writer.close()
			return

		# calculate protocol extensions bytes for us and remote peer
		reserved = merge_reserved(LOCAL_RESERVED, remote_reserved)
		await self._add_peer(info_hash, peer_info, remote_peer_id, reader, writer, reserved)

	async def _connect(self, my_peer_id: bytes, info_hash: bytes, peer_info: PeerInfo, ):
		result = await net.connect(peer_info, info_hash, my_peer_id, reserved=LOCAL_RESERVED)
		if not result:
			self.manager.mark_failed(peer_info)
			return

		remote_peer_id, reader, writer, remote_reserved = result
		reserved = merge_reserved(LOCAL_RESERVED, remote_reserved)

		await self._add_peer(info_hash, peer_info, remote_peer_id, reader, writer, reserved)

	async def _add_peer(self, info_hash: bytes, peer_info: PeerInfo, remote_peer_id: bytes,
	                    reader: StreamReader, writer: StreamWriter, reserved: bytes) -> None:

		ds = self.env.data_storage

		connection = net.Connection(remote_peer_id, reader, writer)

		# send a BITFIELD message first
		torrent_entity: Entity = ds.get_collection(TorrentHashEC).find(info_hash)
		local_bitfield = torrent_entity.get_component(BitfieldEC)
		if local_bitfield.have_num > 0:
			torrent_info_ec = torrent_entity.get_component(TorrentInfoEC)
			await connection.send(bitfield(local_bitfield.dump(torrent_info_ec.info.pieces_num)))

		# create peer entity
		peer_entity = ds.create_entity()
		peer_entity.add_component(PeerInfoEC(info_hash, peer_info))
		peer_entity.add_component(BitfieldEC())
		peer_entity.add_component(PeerConnectionEC(connection, reserved))

		# notify systems about a new peer
		# wait for it before start listening to messages
		await asyncio.gather(
			*self.env.event_bus.dispatch("peer.connected", torrent_entity, peer_entity)
		)

		# start listening to messages
		peer_entity.get_component(PeerConnectionEC).task = asyncio.create_task(
			self._read_messages(torrent_entity, peer_entity))

	async def _read_messages(self, torrent_entity: Entity, peer_entity: Entity):
		peer_info = peer_entity.get_component(PeerInfoEC).peer_info
		self.manager.mark_good(peer_info)

		connection = peer_entity.get_component(PeerConnectionEC).connection

		def on_message(message: net.Message):
			if not torrent_entity.is_valid():
				return
			self.env.event_bus.dispatch("peer.message", torrent_entity, peer_entity, message)
			logger.error(f"Error while reading message from peer {peer_info}: {e}")

		# main peer loop
		while True:
			if connection.is_dead():
				logger.debug("Peer diconnected by other side or timeout")
				break

			# read the next message. return False in case of error
			if await connection.read(on_message):
				continue

			break

		self.manager.mark_failed(peer_info)
		peer_entity.add_component(PeerDisconnectedEC())


def _disconnect_peers(peers: Iterator[Entity]):
	for peer_entity in peers:
		peer_entity.add_component(PeerDisconnectedEC())
