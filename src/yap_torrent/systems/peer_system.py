import asyncio
import logging
import time
from asyncio import StreamReader, StreamWriter, Server
from typing import Iterable, Set, List

from angelovich.core.DataStorage import Entity

import yap_torrent.protocol.connection as net
from yap_torrent.components.peer_ec import PeerConnectionEC, KnownPeersEC, PeerDisconnectedEC
from yap_torrent.components.torrent_ec import TorrentInfoEC, TorrentEC, TorrentStatsEC, TorrentState
from yap_torrent.env import Env
from yap_torrent.protocol import extensions
from yap_torrent.protocol.bt_main_messages import bitfield
from yap_torrent.protocol.extensions import create_reserved, merge_reserved
from yap_torrent.protocol.structures import PeerInfo
from yap_torrent.system import System
from yap_torrent.systems import iterate_peers, is_torrent_active, is_torrent_complete, get_torrent_entity

logger = logging.getLogger(__name__)

# TODO: build dynamically from systems
LOCAL_RESERVED = create_reserved(extensions.DHT, extensions.EXTENSION_PROTOCOL)


class PeerSystem(System):

	def __init__(self, env: Env):
		super().__init__(env)
		self.server: Server = None

	async def start(self):
		port = self.env.config.port
		host = self.env.ip
		self.server = await asyncio.start_server(self._server_callback, host, port)

		self.env.event_bus.add_listener("peers.update", self._on_peers_update, scope=self)
		self.env.event_bus.add_listener("action.torrent.complete", self._on_torrent_complete, scope=self)
		self.env.event_bus.add_listener("action.torrent.stop", self._on_torrent_stop, scope=self)
		self.env.event_bus.add_listener("action.torrent.start", self._on_torrent_start, scope=self)

	def close(self):
		# TODO: disconnect all peers

		self.server.close()
		self.env.event_bus.remove_all_listeners(scope=self)
		super().close()

	def process_disconnected(self):
		ds = self.env.data_storage
		to_remove = ds.get_collection(PeerDisconnectedEC).entities
		for peer_entity in to_remove:
			peer_ec = peer_entity.get_component(PeerConnectionEC)
			logger.info("Disconnect %s", peer_ec)
			peer_ec.disconnect()
			ds.remove_entity(peer_entity)

	def remove_outdated_peers(self):
		for peer_entity in self.env.data_storage.get_collection(PeerConnectionEC):
			peer_ec = peer_entity.get_component(PeerConnectionEC)

			# Remote peer interested. don't remove'
			if peer_ec.remote_interested:
				continue

			# I'm interested in this peer. don't remove
			if peer_ec.local_interested:
				continue

			# just connected. keep it alive for a while
			if time.monotonic() - peer_ec.connection.connection_time < 30:
				continue

			logger.info("Removing outdated peer %s", peer_ec)
			peer_entity.add_component(PeerDisconnectedEC())

	def overflow_check(self):
		peers_count = len(self.env.data_storage.get_collection(PeerConnectionEC))
		if peers_count <= self.env.config.max_connections:
			return
		logger.info("Too much connected peers: %s", peers_count)

		def sort_key(_e: Entity):
			peer_ec = _e.get_component(PeerConnectionEC)
			return int(peer_ec.local_interested), int(peer_ec.remote_interested), peer_ec.connection.last_message_time

		to_remove = sorted((e for e in self.env.data_storage.get_collection(PeerConnectionEC)), key=sort_key)[
			:-self.env.config.max_connections]
		for peer_entity in to_remove:
			logger.info("Max capacity disconnect: %s", peer_entity.get_component(PeerConnectionEC))
			peer_entity.add_component(PeerDisconnectedEC())

	def connect_to_peers(self):
		ds = self.env.data_storage
		my_peer_id = self.env.peer_id

		active_hosts: Set[str] = set(
			d.get_component(PeerConnectionEC).peer_info.host
			for d in ds.get_collection(PeerConnectionEC)
		)

		# select only torrents we want to download
		active_torrents: List[Entity] = [
			e for e in ds.get_collection(TorrentEC)
			if is_torrent_active(e) and not is_torrent_complete(e)
		]
		# TODO: sort active torrents by priority

		for torrent_entity in active_torrents:
			for peer in torrent_entity.get_component(KnownPeersEC).get_peers_to_connect(active_hosts):
				if len(active_hosts) >= self.env.config.max_connections:
					return
				active_hosts.add(peer.host)
				info_hash = torrent_entity.get_component(TorrentEC).info_hash
				self.add_task(self._connect(my_peer_id, info_hash, peer))

	async def _update(self, delta_time: float):
		ds = self.env.data_storage

		# cleanup disconnected peers:
		self.process_disconnected()

		# remove peers with no interest or no response
		self.remove_outdated_peers()

		# incoming connections can overflow the limit. clean it up
		self.overflow_check()

		# check capacity first
		if len(ds.get_collection(PeerConnectionEC)) >= self.env.config.max_connections:
			return

		# try to connect to any peers we know about
		self.connect_to_peers()

	async def _on_torrent_complete(self, torrent_entity: Entity):
		# TODO: replace with update logic
		info_hash = torrent_entity.get_component(TorrentEC).info_hash
		logger.info("Disconnect on torrent complete")
		_disconnect_peers(
			p for p in iterate_peers(self.env, info_hash) if
			not p.get_component(PeerConnectionEC).remote_interested
		)

	async def _on_torrent_stop(self, info_hash: bytes):
		logger.info("Disconnect on torrent stop")
		_disconnect_peers(p for p in iterate_peers(self.env, info_hash))

	async def _on_torrent_start(self, info_hash: bytes):
		pass

	async def _on_peers_update(self, info_hash: bytes, peers: Iterable[PeerInfo]):
		torrent_entity = get_torrent_entity(self.env, info_hash)
		if not torrent_entity:
			return

		torrent_entity.get_component(KnownPeersEC).update_peers(peers)

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
		torrent_entity = self.env.data_storage.get_collection(TorrentEC).find(info_hash)
		if not torrent_entity:
			logger.debug("%s asks for torrent %s we don't have", peer_info, info_hash)
			writer.close()
			return

		# calculate protocol extensions bytes for us and remote peer
		reserved = merge_reserved(LOCAL_RESERVED, remote_reserved)
		await self._add_peer(info_hash, peer_info, remote_peer_id, reader, writer, reserved)

	async def _connect(self, my_peer_id: bytes, info_hash: bytes, peer_info: PeerInfo):
		result = await net.connect(peer_info, info_hash, my_peer_id, reserved=LOCAL_RESERVED)
		if not result:
			get_torrent_entity(self.env, info_hash).get_component(KnownPeersEC).mark_failed(peer_info)
			return

		remote_peer_id, reader, writer, remote_reserved = result
		reserved = merge_reserved(LOCAL_RESERVED, remote_reserved)

		await self._add_peer(info_hash, peer_info, remote_peer_id, reader, writer, reserved)

	async def _add_peer(self, info_hash: bytes, peer_info: PeerInfo, remote_peer_id: bytes,
	                    reader: StreamReader, writer: StreamWriter, reserved: bytes) -> None:
		ds = self.env.data_storage
		connection = net.Connection(remote_peer_id, reader, writer)
		torrent_entity: Entity = ds.get_collection(TorrentEC).find(info_hash)

		# disconnect in case of inactive torrents
		if torrent_entity.get_component(TorrentStatsEC).state == TorrentState.Inactive:
			logger.debug("%s connected to inactive torrent %s. Disconnecting", peer_info, info_hash.hex())
			connection.close()
			return

		# send a BITFIELD message first
		local_bitfield = torrent_entity.get_component(TorrentEC).bitfield
		if local_bitfield.have_num > 0:
			torrent_info_ec = torrent_entity.get_component(TorrentInfoEC)
			await connection.send(bitfield(local_bitfield.dump(torrent_info_ec.info.pieces_num)))

		# create peer entity
		peer_entity = ds.create_entity().add_component(PeerConnectionEC(info_hash, peer_info, connection, reserved))

		# notify systems about a new peer
		# wait for it before start listening to messages
		await asyncio.gather(
			*self.env.event_bus.dispatch("peer.connected", torrent_entity, peer_entity)
		)

		# start listening to messages
		peer_entity.get_component(PeerConnectionEC).task = asyncio.create_task(
			self._read_messages(torrent_entity, peer_entity))

	async def _read_messages(self, torrent_entity: Entity, peer_entity: Entity):
		peer_info = peer_entity.get_component(PeerConnectionEC).peer_info
		connection = peer_entity.get_component(PeerConnectionEC).connection

		known_peers_ec = torrent_entity.get_component(KnownPeersEC)

		def on_message(message: net.Message):
			# ignore messages for inactive torrents
			if not is_torrent_active(torrent_entity):
				return
			self.env.event_bus.dispatch("peer.message", torrent_entity, peer_entity, message)
			known_peers_ec.mark_good(peer_info)

		# main peer loop
		while True:
			if connection.is_dead():
				break

			# read the next message. return False in case of error
			if await connection.read(on_message):
				continue

			torrent_entity.get_component(KnownPeersEC).mark_failed(peer_info)
			break

		logger.info("No more messages %s", peer_info.host)
		peer_entity.add_component(PeerDisconnectedEC())


def _disconnect_peers(peers: Iterable[Entity]):
	for peer_entity in peers:
		peer_entity.add_component(PeerDisconnectedEC())
