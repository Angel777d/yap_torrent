import asyncio
import logging
import time
from asyncio import StreamReader, StreamWriter, Server
from typing import Iterable, List

from angelovich.core.DataStorage import Entity

import yap_torrent.protocol.connection as net
from yap_torrent.components.peer_ec import (
	FullPeerEC,
	FreePeerEC,
	LocalInterestedEC,
	LocalUnchokedEC,
	PeerConnectingEC,
	PeerConnectionEC,
	PeerDisconnectedEC,
	PeerEC,
	PeerState,
	PeerStatsEC,
	RemoteInterestedEC,
	RemoteUnchokedEC,
)
from yap_torrent.components.torrent_ec import TorrentInfoEC, TorrentEC, TorrentStatsEC, TorrentState
from yap_torrent.env import Env
from yap_torrent.protocol import extensions
from yap_torrent.protocol.bt_main_messages import bitfield
from yap_torrent.protocol.extensions import create_reserved, merge_reserved
from yap_torrent.protocol.structures import PeerInfo
from yap_torrent.system import System
from yap_torrent.systems import (
	add_known_peer,
	get_info_hash,
	get_torrent_entity,
	is_torrent_active,
	is_torrent_complete,
	iterate_peer_entities,
	iterate_peers,
)
from yap_torrent.systems.peer_logic import next_state_on_failure, should_attempt

logger = logging.getLogger(__name__)

# TODO: build dynamically from systems
LOCAL_RESERVED = create_reserved(extensions.DHT, extensions.EXTENSION_PROTOCOL)

# components attached only while a peer is connected — cleared on disconnect (PeerEC stays)
_CONNECTION_COMPONENTS = (
	PeerConnectionEC, PeerStatsEC, PeerConnectingEC,
	LocalInterestedEC, RemoteUnchokedEC, RemoteInterestedEC, LocalUnchokedEC,
	FullPeerEC, FreePeerEC,
)


def _in_download_queue(peer_entity: Entity) -> bool:
	return peer_entity.has_component(LocalInterestedEC) and peer_entity.has_component(RemoteUnchokedEC)


def _in_upload_queue(peer_entity: Entity) -> bool:
	return peer_entity.has_component(LocalUnchokedEC) and peer_entity.has_component(RemoteInterestedEC)


def _in_any_queue(peer_entity: Entity) -> bool:
	return _in_download_queue(peer_entity) or _in_upload_queue(peer_entity)


class PeerSystem(System):

	def __init__(self, env: Env):
		super().__init__(env)
		self.server: Server = None

	async def start(self):
		port = self.env.config.port
		host = self.env.ip
		self.server = await asyncio.start_server(self._server_callback, host, port)

		self.add_listener("peers.update", self._on_peers_update)
		self.add_listener("action.torrent.complete", self._on_torrent_complete)
		self.add_listener("action.torrent.stop", self._on_torrent_stop)
		self.add_listener("action.torrent.start", self._on_torrent_start)

	def close(self):
		# disconnect all peers so their read-loop tasks are cancelled
		for peer_entity in list(self.env.data_storage.get_collection(PeerConnectionEC)):
			peer_entity.get_component(PeerConnectionEC).disconnect()
		if self.server:
			self.server.close()
		super().close()

	async def _update(self, delta_time: float):
		ds = self.env.data_storage

		await self._process_disconnected()
		self._drop_suspicious()
		self._drop_idle_connections()
		self._overflow_check()

		if len(ds.get_collection(PeerConnectionEC)) >= self.env.config.max_connections:
			return
		self._connect_to_peers()

	# -- connection teardown ----------------------------------------------
	async def _process_disconnected(self):
		ds = self.env.data_storage
		for peer_entity in ds.get_collection(PeerDisconnectedEC).entities:
			if peer_entity.has_component(PeerConnectionEC):
				# let owners (e.g. DownloadSystem) release the peer's in-flight work
				# BEFORE its connection components are torn down
				info_hash = peer_entity.get_component(PeerConnectionEC).info_hash
				torrent_entity = get_torrent_entity(self.env, info_hash)
				if torrent_entity is not None:
					await self.env.event_bus.dispatch_async("peer.disconnected", torrent_entity, peer_entity)
				logger.debug("Disconnect %s", peer_entity.get_component(PeerConnectionEC))
				peer_entity.get_component(PeerConnectionEC).disconnect()
			for component in _CONNECTION_COMPONENTS:
				if peer_entity.has_component(component):
					peer_entity.remove_component(component)
			if peer_entity.has_component(PeerDisconnectedEC):
				peer_entity.remove_component(PeerDisconnectedEC)

	def _drop_suspicious(self):
		for peer_entity in list(self.env.data_storage.get_collection(PeerConnectionEC)):
			if peer_entity.get_component(PeerEC).state == PeerState.Suspicious:
				_mark_disconnected(peer_entity)

	def _drop_idle_connections(self):
		# drop a connection after 30s if the peer is in neither the download nor upload queue
		for peer_entity in list(self.env.data_storage.get_collection(PeerConnectionEC)):
			if _in_any_queue(peer_entity):
				continue
			if time.monotonic() - peer_entity.get_component(PeerConnectionEC).connection_time < 30:
				continue
			_mark_disconnected(peer_entity)

	def _overflow_check(self):
		ds = self.env.data_storage
		connected = [e for e in ds.get_collection(PeerConnectionEC)]
		if len(connected) <= self.env.config.max_connections:
			return

		def sort_key(peer_entity: Entity):
			conn = peer_entity.get_component(PeerConnectionEC)
			return int(_in_any_queue(peer_entity)), conn.connection.last_message_time

		# keep the top max_connections (in-queue / recently active); drop the rest
		to_remove = sorted(connected, key=sort_key)[:-self.env.config.max_connections]
		for peer_entity in to_remove:
			_mark_disconnected(peer_entity)

	# -- outbound connections (state machine) ------------------------------
	def _connect_to_peers(self):
		ds = self.env.data_storage
		my_peer_id = self.env.peer_id
		now = time.monotonic()

		connected = len(ds.get_collection(PeerConnectionEC))
		limit = self.env.config.max_connections

		active_torrents: List[Entity] = [
			e for e in ds.get_collection(TorrentEC)
			if is_torrent_active(e) and not is_torrent_complete(e)
		]

		for torrent_entity in active_torrents:
			info_hash = get_info_hash(torrent_entity)
			# each peer entity is unique per (info_hash, host, port), so connect to any
			# that is neither connected nor already being connected
			for peer_entity in iterate_peer_entities(self.env, info_hash):
				if connected >= limit:
					return
				peer_ec = peer_entity.get_component(PeerEC)
				if peer_entity.has_component(PeerConnectionEC) or peer_entity.has_component(PeerConnectingEC):
					continue
				if not should_attempt(peer_ec.state, peer_ec.last_attempt, now):
					continue

				connected += 1
				peer_ec.last_attempt = now
				peer_entity.add_component(PeerConnectingEC())
				self.add_task(self._connect(my_peer_id, info_hash, peer_entity))

	async def _connect(self, my_peer_id: bytes, info_hash: bytes, peer_entity: Entity):
		peer_ec = peer_entity.get_component(PeerEC)
		peer_info = peer_ec.peer_info
		try:
			result = await net.connect(peer_info, info_hash, my_peer_id, reserved=LOCAL_RESERVED)
			if not result:
				peer_ec.state, peer_ec.fail_count = next_state_on_failure(peer_ec.state, peer_ec.fail_count)
				return

			remote_peer_id, reader, writer, remote_reserved = result
			reserved = merge_reserved(LOCAL_RESERVED, remote_reserved)
			# good right after a successful handshake
			peer_ec.state = PeerState.Good
			peer_ec.fail_count = 0
			await self._add_peer(info_hash, peer_entity, remote_peer_id, reader, writer, reserved)
		finally:
			if peer_entity.has_component(PeerConnectingEC):
				peer_entity.remove_component(PeerConnectingEC)

	# -- inbound connections ----------------------------------------------
	async def _server_callback(self, reader: StreamReader, writer: StreamWriter):
		peer_info = PeerInfo(*writer.transport.get_extra_info('peername'))
		logger.debug('%s connected to us', peer_info)

		local_peer_id = self.env.peer_id
		result = await net.on_connect(local_peer_id, reader, writer, LOCAL_RESERVED)
		if result is None:
			return

		pstrlen, pstr, remote_reserved, info_hash, remote_peer_id = result

		torrent_entity = get_torrent_entity(self.env, info_hash)
		if not torrent_entity:
			logger.debug("%s asks for torrent %s we don't have", peer_info, info_hash)
			writer.close()
			return

		peer_entity = add_known_peer(self.env, info_hash, peer_info)
		peer_ec = peer_entity.get_component(PeerEC)
		if peer_ec.state == PeerState.Suspicious or peer_entity.has_component(PeerConnectionEC):
			writer.close()
			return
		peer_ec.state = PeerState.Good
		peer_ec.fail_count = 0

		reserved = merge_reserved(LOCAL_RESERVED, remote_reserved)
		await self._add_peer(info_hash, peer_entity, remote_peer_id, reader, writer, reserved)

	async def _add_peer(self, info_hash: bytes, peer_entity: Entity, remote_peer_id: bytes,
	                    reader: StreamReader, writer: StreamWriter, reserved: bytes) -> None:
		connection = net.Connection(remote_peer_id, reader, writer)
		torrent_entity = get_torrent_entity(self.env, info_hash)

		if torrent_entity is None or torrent_entity.get_component(TorrentStatsEC).state == TorrentState.Inactive:
			logger.debug("%s connected to inactive/unknown torrent %s. Disconnecting",
			             peer_entity.get_component(PeerEC).peer_info, info_hash.hex())
			connection.close()
			return

		peer_info = peer_entity.get_component(PeerEC).peer_info

		# send a BITFIELD message first
		local_bitfield = torrent_entity.get_component(TorrentEC).bitfield
		if local_bitfield.have_num > 0 and torrent_entity.has_component(TorrentInfoEC):
			info = torrent_entity.get_component(TorrentInfoEC).info
			await connection.send(bitfield(local_bitfield.dump(info.pieces_num)))

		# attach the live connection to the (persistent) peer entity
		peer_entity.add_component(PeerConnectionEC(info_hash, peer_info, connection, reserved))
		peer_entity.add_component(PeerStatsEC())

		await asyncio.gather(*self.env.event_bus.dispatch("peer.connected", torrent_entity, peer_entity))

		peer_entity.get_component(PeerConnectionEC).task = asyncio.create_task(
			self._read_messages(torrent_entity, peer_entity))

	async def _read_messages(self, torrent_entity: Entity, peer_entity: Entity):
		connection = peer_entity.get_component(PeerConnectionEC).connection
		peer_info = peer_entity.get_component(PeerConnectionEC).peer_info

		def on_message(message: net.Message):
			if not is_torrent_active(torrent_entity):
				return
			self.env.event_bus.dispatch("peer.message", torrent_entity, peer_entity, message)

		while True:
			if connection.is_dead():
				break
			if await connection.read(on_message):
				continue
			break

		logger.debug("No more messages %s", peer_info.host)
		_mark_disconnected(peer_entity)

	# -- events ------------------------------------------------------------
	async def _on_torrent_complete(self, torrent_entity: Entity):
		info_hash = get_info_hash(torrent_entity)
		logger.debug("Disconnect uninterested peers on torrent complete")
		_disconnect_peers(
			p for p in iterate_peers(self.env, info_hash)
			if not p.has_component(RemoteInterestedEC)
		)

	async def _on_torrent_stop(self, torrent_entity: Entity):
		info_hash = get_info_hash(torrent_entity)
		logger.debug("Disconnect all peers on torrent stop")
		_disconnect_peers(iterate_peers(self.env, info_hash))

	async def _on_torrent_start(self, torrent_entity: Entity):
		pass

	async def _on_peers_update(self, info_hash: bytes, peers: Iterable[PeerInfo]):
		if not get_torrent_entity(self.env, info_hash):
			return
		for peer_info in peers:
			add_known_peer(self.env, info_hash, peer_info)


def _mark_disconnected(peer_entity: Entity):
	if not peer_entity.has_component(PeerDisconnectedEC):
		peer_entity.add_component(PeerDisconnectedEC())


def _disconnect_peers(peers: Iterable[Entity]):
	for peer_entity in peers:
		_mark_disconnected(peer_entity)
