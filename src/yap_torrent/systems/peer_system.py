import asyncio
import logging
import time
from asyncio import StreamReader, StreamWriter, Server
from typing import Iterable, List

from angelovich.core.DataStorage import Entity

import yap_torrent.protocol.connection as net
from yap_torrent.components.peer_ec import (
	FullPeerEC,
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
from yap_torrent.components.torrent_ec import TorrentDownloadProgressEC, TorrentInfoEC, TorrentEC, TorrentStatsEC, \
	TorrentState
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
	iterate_peer_entities,
	iterate_peers,
)
from yap_torrent.systems.intrest_system import interested_pieces
from yap_torrent.systems.peer_logic import next_state_on_failure, should_attempt

logger = logging.getLogger(__name__)

# TODO: build dynamically from systems
LOCAL_RESERVED = create_reserved(extensions.DHT, extensions.EXTENSION_PROTOCOL)

# Components attached only while a peer is connected — cleared on disconnect (PeerEC stays).
# PeerConnectingEC is deliberately NOT here: it is owned by the in-flight _connect task and
# stripping it early would let _connect_to_peers dial a peer that is already being dialled.
_CONNECTION_COMPONENTS = (
	PeerConnectionEC, PeerStatsEC,
	LocalInterestedEC, RemoteUnchokedEC, RemoteInterestedEC, LocalUnchokedEC,
	FullPeerEC,
)


def _in_download_queue(peer_entity: Entity) -> bool:
	return peer_entity.has_component(LocalInterestedEC) and peer_entity.has_component(RemoteUnchokedEC)


def _in_upload_queue(peer_entity: Entity) -> bool:
	return peer_entity.has_component(LocalUnchokedEC) and peer_entity.has_component(RemoteInterestedEC)


def _in_any_queue(peer_entity: Entity) -> bool:
	return _in_download_queue(peer_entity) or _in_upload_queue(peer_entity)


def download_value(torrent_entity: Entity, peer_entity: Entity) -> int:
	"""How many wanted pieces this peer could give us."""
	return len(interested_pieces(torrent_entity, peer_entity.get_component(PeerEC).remote_bitfield))


def upload_value(torrent_entity: Entity, peer_entity: Entity) -> int:
	"""How many pieces of our wanted set this peer lacks.

	Wanted rather than held: two fresh leechers hold nothing, so a have-based score is 0
	both ways and neither would ever dial the other.
	"""
	if torrent_entity.has_component(TorrentDownloadProgressEC):
		local = torrent_entity.get_component(TorrentDownloadProgressEC).wanted
	else:
		local = torrent_entity.get_component(TorrentEC).bitfield

	return len(peer_entity.get_component(PeerEC).remote_bitfield.interested_in(local))


def _has_metadata(torrent_entity: Entity) -> bool:
	return torrent_entity.has_component(TorrentInfoEC)


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
		self.add_listener("action.torrent.remove", self._on_torrent_remove)

	def close(self):
		# disconnect all peers so their read-loop tasks are cancelled
		for peer_entity in list(self.env.data_storage.get_collection(PeerConnectionEC)):
			peer_entity.get_component(PeerConnectionEC).disconnect()
		if self.server:
			self.server.close()
		super().close()

	async def _update(self, delta_time: float):
		await self._process_disconnected()
		self._drop_suspicious()
		self._drop_idle_connections()
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
		"""Drop connections that stay out of both queues: nothing to fetch, nothing to serve."""
		timeout = self.env.config.peer_idle_timeout
		now = time.monotonic()
		for peer_entity in list(self.env.data_storage.get_collection(PeerConnectionEC)):
			conn = peer_entity.get_component(PeerConnectionEC)
			if _in_any_queue(peer_entity):
				conn.last_queue_time = now
				continue
			torrent_entity = get_torrent_entity(self.env, conn.info_hash)
			if torrent_entity is not None and not _has_metadata(torrent_entity):
				continue  # this peer is the magnet's only metadata source
			if now - conn.last_queue_time < timeout:
				continue
			_mark_disconnected(peer_entity)

	# -- outbound connections (state machine) ------------------------------
	def _connect_to_peers(self):
		ds = self.env.data_storage
		my_peer_id = self.env.peer_id
		now = time.monotonic()

		# complete torrents are included: seeding still needs upload-slot connections
		active_torrents: List[Entity] = [e for e in ds.get_collection(TorrentEC) if is_torrent_active(e)]

		for torrent_entity in active_torrents:
			info_hash = get_info_hash(torrent_entity)
			free_download, free_upload = self._free_slots(torrent_entity)
			if free_download <= 0 and free_upload <= 0:
				continue

			for peer_entity in self._connect_candidates(torrent_entity, free_download, free_upload, now):
				peer_ec = peer_entity.get_component(PeerEC)
				if not should_attempt(peer_ec.state, peer_ec.last_attempt, now):
					continue

				peer_ec.last_attempt = now
				peer_entity.add_component(PeerConnectingEC())
				self.add_task(self._connect(my_peer_id, info_hash, peer_entity))

	def _free_slots(self, torrent_entity: Entity) -> tuple[int, int]:
		if not _has_metadata(torrent_entity):
			# a magnet has no queues yet, so never report it as full
			return self.env.config.download_peers_limit, self.env.config.upload_peers_limit

		download = self.env.config.download_peers_limit
		upload = self.env.config.upload_peers_limit
		for peer_entity in iterate_peers(self.env, get_info_hash(torrent_entity)):
			if _in_download_queue(peer_entity):
				download -= 1
			if _in_upload_queue(peer_entity):
				upload -= 1
		return download, upload

	def _connect_candidates(self, torrent_entity: Entity, free_download: int, free_upload: int,
	                        now: float) -> List[Entity]:
		"""Disconnected peers worth dialling, best first, scored from their stored bitfield.

		A download score is evidence: the peer's bitfield showed us pieces we want. An upload
		score is a guess — a peer that told us nothing scores as lacking everything — so it
		is held off by upload_retry_cooldown to avoid a drop/redial loop.
		"""
		# TODO: hand out candidates a few per tick. _connect_to_peers consumes this whole
		#  list, so a tracker or DHT reply carrying hundreds of peers dials them all in one
		#  tick (should_attempt does not throttle Unknown peers, whose retry delay is 0).
		has_metadata = _has_metadata(torrent_entity)
		cooldown = self.env.config.upload_retry_cooldown
		candidates: List[tuple[int, Entity]] = []

		for peer_entity in iterate_peer_entities(self.env, get_info_hash(torrent_entity)):
			if peer_entity.has_component(PeerConnectionEC) or peer_entity.has_component(PeerConnectingEC):
				continue

			if not has_metadata:
				candidates.append((1, peer_entity))
				continue

			download = download_value(torrent_entity, peer_entity) if free_download > 0 else 0
			upload = upload_value(torrent_entity, peer_entity) if free_upload > 0 else 0

			if upload and now - peer_entity.get_component(PeerEC).last_attempt < cooldown:
				upload = 0

			value = max(download, upload)
			if value > 0:
				candidates.append((value, peer_entity))

		candidates.sort(key=lambda item: item[0], reverse=True)
		return [peer_entity for _, peer_entity in candidates]

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
			return  # on_connect closed the stream on its way out

		pstrlen, pstr, remote_reserved, info_hash, remote_peer_id = result

		torrent_entity = get_torrent_entity(self.env, info_hash)
		if not torrent_entity:
			logger.debug("%s asks for torrent %s we don't have", peer_info, info_hash)
			net.close_writer(writer)
			return

		peer_entity = add_known_peer(self.env, info_hash, peer_info)
		peer_ec = peer_entity.get_component(PeerEC)
		# PeerConnectingEC matters here: without it a simultaneous connect (we dial them
		# while they dial us) would attach a second connection to the same peer entity
		if (peer_ec.state == PeerState.Suspicious
				or peer_entity.has_component(PeerConnectionEC)
				or peer_entity.has_component(PeerConnectingEC)):
			net.close_writer(writer)
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

		# add_component is a no-op when the type is already present, so a second connection
		# would be silently dropped on the floor with its socket still open
		if not peer_entity.is_valid() or peer_entity.has_component(PeerConnectionEC):
			logger.debug("Peer %s is already connected. Dropping the duplicate",
			             peer_entity.get_component(PeerEC).peer_info if peer_entity.is_valid() else "?")
			connection.close()
			return

		peer_info = peer_entity.get_component(PeerEC).peer_info

		# send a BITFIELD message first
		local_bitfield = torrent_entity.get_component(TorrentEC).bitfield
		if local_bitfield.have_num > 0 and torrent_entity.has_component(TorrentInfoEC):
			info = torrent_entity.get_component(TorrentInfoEC).info
			try:
				await connection.send(bitfield(local_bitfield.dump(info.pieces_num)))
			except Exception as ex:  # noqa: BLE001
				logger.debug("Failed to send bitfield to %s: %s", peer_info, ex)
				connection.close()
				return

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

	async def _on_torrent_remove(self, info_hash: bytes):
		"""Drop the swarm with the torrent: disconnect every peer, then delete its entity.

		Runs while the torrent entity still exists (TorrentSystem removes it after this
		event), so peer.disconnected can still be delivered with a valid torrent — the
		deferred _process_disconnected path would arrive too late to do that.
		"""
		ds = self.env.data_storage
		torrent_entity = get_torrent_entity(self.env, info_hash)

		for peer_entity in list(iterate_peer_entities(self.env, info_hash)):
			if peer_entity.has_component(PeerConnectionEC):
				if torrent_entity is not None:
					await self.env.event_bus.dispatch_async("peer.disconnected", torrent_entity, peer_entity)
				peer_entity.get_component(PeerConnectionEC).disconnect()
			ds.remove_entity(peer_entity)

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
