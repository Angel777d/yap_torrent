import asyncio
import logging
import math
import time
from asyncio import StreamReader, StreamWriter, Server
from typing import Iterable, Optional, Tuple

from angelovich.core.DataStorage import Entity, DataStorage

import yap_torrent.protocol.connection as net
from yap_torrent.components.common import IdleEC
from yap_torrent.components.peer_ec import (
	LocalInterestedEC,
	LocalUnchokedEC,
	PeerConnectionInProgressEC,
	PeerConnectionEC,
	PeerDisconnectedEC,
	PeerEC,
	PeerRateEC,
	PeerState,
	RemoteInterestedEC,
	RemoteUnchokedEC, PeerPendingRemoveEC,
)
from yap_torrent.components.torrent_ec import TorrentDownloadProgressEC, TorrentInfoEC, TorrentEC, TorrentStatsEC, \
	TorrentState, TorrentQueuePositionEC
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
	iterate_peers,
	iterate_connected_peers, )
from yap_torrent.systems.intrest_system import interested_pieces
from yap_torrent.systems.peer_logic import next_state_on_failure, should_attempt

logger = logging.getLogger(__name__)

# TODO: build dynamically from systems
LOCAL_RESERVED = create_reserved(extensions.DHT, extensions.EXTENSION_PROTOCOL)

MAX_METADATA_CANDIDATES_PER_TICK = 10

_CONNECTION_COMPONENTS = (
	PeerConnectionEC, PeerRateEC,
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
	"""How many pieces of our wanted set this peer lacks (wanted, not held)."""
	if torrent_entity.has_component(TorrentDownloadProgressEC):
		local = torrent_entity.get_component(TorrentDownloadProgressEC).wanted
	else:
		local = torrent_entity.get_component(TorrentEC).bitfield

	return len(peer_entity.get_component(PeerEC).remote_bitfield.interested_in(local))


def _has_metadata(torrent_entity: Entity) -> bool:
	return torrent_entity.has_component(TorrentInfoEC)


def _calculate_queue_sizes(ds: DataStorage) -> tuple[int, int]:
	download = 0
	upload = 0
	for e in ds.get_collection(PeerConnectionEC):
		download += _in_download_queue(e)
		upload += _in_upload_queue(e)
	return download, upload


class PeerSystem(System):

	def __init__(self, env: Env):
		super().__init__(env)
		self.server: Optional[Server] = None

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
		await self._process_pending_remove()

		# TODO: add invalidation flag (by time and events )
		self._drop_idle_connections()
		self._connect_to_peers()

	# -- connection teardown ----------------------------------------------
	async def _process_disconnected(self):
		ds = self.env.data_storage
		for peer_entity in ds.get_collection(PeerDisconnectedEC).entities:
			if peer_entity.has_component(PeerConnectionEC):
				# let owners release the peer's work before its connection is torn down
				info_hash = peer_entity.get_component(PeerConnectionEC).info_hash
				torrent_entity = get_torrent_entity(self.env, info_hash)
				if torrent_entity:
					await self.env.event_bus.dispatch_async("peer.disconnected", torrent_entity, peer_entity)
				logger.debug("Disconnect %s", peer_entity.get_component(PeerConnectionEC))
				peer_entity.get_component(PeerConnectionEC).disconnect()
			for component in _CONNECTION_COMPONENTS:
				if peer_entity.has_component(component):
					peer_entity.remove_component(component)

			peer_entity.remove_component(PeerDisconnectedEC)

	async def _process_pending_remove(self):
		ds = self.env.data_storage
		for peer_entity in ds.get_collection(PeerPendingRemoveEC).entities:
			ds.remove_entity(peer_entity)

	def _drop_idle_connections(self):
		timeout = self.env.config.peer_idle_timeout
		for peer_entity in list(self.env.data_storage.get_collection(PeerConnectionEC)):
			torrent_entity = get_torrent_entity(self.env, peer_entity.get_component(PeerEC).info_hash)
			if torrent_entity and not _has_metadata(torrent_entity):
				continue  # this peer is the magnet's only metadata source

			idle = peer_entity.get_component(IdleEC)
			if _in_any_queue(peer_entity):
				idle.touch()
				continue
			if not idle.overlives_period(timeout):
				continue
			_mark_disconnected(peer_entity)

	# -- outbound connections (state machine) ------------------------------
	def _connect_to_peers(self):
		now = time.monotonic()
		my_peer_id = self.env.peer_id

		for peer_entity in calculate_candidates(self.env, now):
			peer_ec = peer_entity.get_component(PeerEC)
			peer_ec.last_attempt = now
			peer_entity.add_component(PeerConnectionInProgressEC())
			self.add_task(self._connect(my_peer_id, peer_ec.info_hash, peer_entity))

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

			peer_ec.state = PeerState.Good
			peer_ec.fail_count = 0
			peer_ec.can_reach = True  # this address listens: we just reached it
			await self._add_peer(info_hash, peer_entity, remote_peer_id, reader, writer, reserved)
		finally:
			peer_entity.remove_component(PeerConnectionInProgressEC)

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

		if (peer_ec.state == PeerState.Suspicious
				or peer_entity.has_component(PeerConnectionEC)
				or peer_entity.has_component(PeerConnectionInProgressEC)):
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

		if torrent_entity is None or not is_torrent_active(torrent_entity):
			logger.debug("%s connected to inactive/unknown torrent %s. Disconnecting",
			             peer_entity.get_component(PeerEC).peer_info, info_hash.hex())
			connection.close()
			return

		# add_component is a no-op on a duplicate type, so guard the second connection
		if not peer_entity.is_valid() or peer_entity.has_component(PeerConnectionEC):
			logger.debug("Peer %s is already connected. Dropping the duplicate",
			             peer_entity.get_component(PeerEC).peer_info if peer_entity.is_valid() else "?")
			connection.close()
			return

		peer_ec = peer_entity.get_component(PeerEC)
		peer_info = peer_ec.peer_info

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
		peer_entity.add_component(PeerRateEC())
		peer_entity.get_component(IdleEC).touch()
		# re-learn the bitfield from this connection; the stored one is only a guess now
		peer_ec.remote_bitfield.reset(set())

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
			p for p in iterate_connected_peers(self.env, info_hash)
			if not p.has_component(RemoteInterestedEC)
		)

	async def _on_torrent_stop(self, torrent_entity: Entity):
		info_hash = get_info_hash(torrent_entity)
		logger.debug("Disconnect all peers on torrent stop")
		_disconnect_peers(iterate_connected_peers(self.env, info_hash))

	async def _on_torrent_remove(self, info_hash: bytes):
		for peer_entity in list(iterate_peers(self.env, info_hash)):
			_mark_to_remove(peer_entity)

	async def _on_peers_update(self, info_hash: bytes, peers: Iterable[PeerInfo]):
		if not get_torrent_entity(self.env, info_hash):
			return
		for peer_info in peers:
			add_known_peer(self.env, info_hash, peer_info)


def _mark_disconnected(peer_entity: Entity):
	if not peer_entity.has_component(PeerDisconnectedEC):
		peer_entity.add_component(PeerDisconnectedEC())


def _mark_to_remove(peer_entity: Entity):
	if peer_entity.has_component(PeerConnectionEC):
		_mark_disconnected(peer_entity)
	peer_entity.add_component(PeerPendingRemoveEC())


def _disconnect_peers(peers: Iterable[Entity]):
	for peer_entity in peers:
		_mark_disconnected(peer_entity)


def calculate_candidates(env, now):
	ds = env.data_storage
	cooldown = env.config.upload_retry_cooldown
	download_limit = env.config.download_peers_limit
	upload_limit = env.config.upload_peers_limit

	download_size, upload_size = _calculate_queue_sizes(ds)

	download_candidates: list[tuple[int, Entity]] = []
	upload_candidates: list[tuple[int, Entity]] = []
	metadata_candidates: list[Entity] = []
	for peer_entity in ds.get_collection(PeerEC):
		if peer_entity.has_component(PeerPendingRemoveEC):
			continue
		if peer_entity.has_component(PeerConnectionEC):
			continue
		if peer_entity.has_component(PeerConnectionInProgressEC):
			continue
		if not should_attempt(peer_entity.get_component(PeerEC).state, peer_entity.get_component(PeerEC).last_attempt,
		                      now):
			continue

		torrent_entity = get_torrent_entity(env, peer_entity.get_component(PeerEC).info_hash)
		if not torrent_entity:
			continue

		if not is_torrent_active(torrent_entity):
			continue

		if not torrent_entity.has_component(TorrentInfoEC):
			metadata_candidates.append(peer_entity)
			continue

		if download_size < download_limit:
			dv = download_value(torrent_entity, peer_entity)
			if dv:
				download_candidates.append((dv, peer_entity))

		if upload_size < upload_limit:
			uv = upload_value(torrent_entity, peer_entity)
			if uv > 0 and now - peer_entity.get_component(PeerEC).last_attempt >= cooldown:
				upload_candidates.append((uv, peer_entity))

	# info_hash -> queue position; a torrent without one yet sorts last (math.inf)
	priorities = {e.get_component(TorrentEC).info_hash: e.get_component(TorrentQueuePositionEC).position
	              for e in ds.get_collection(TorrentQueuePositionEC)}

	def download_sort(item: Tuple[int, Entity]):
		value, e = item
		return priorities.get(e.get_component(PeerEC).info_hash, math.inf), -value

	return set(e for _, e in sorted(upload_candidates,
	                                key=lambda e: e[0],
	                                reverse=True)[:max(upload_limit - upload_size, 0)]
	           ).union(e for _, e in sorted(download_candidates,
	                                        key=download_sort)[
		:max(download_limit - download_size, 0)]
	                   ).union(metadata_candidates[:MAX_METADATA_CANDIDATES_PER_TICK])
