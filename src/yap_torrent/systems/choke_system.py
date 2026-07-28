import logging
from typing import Iterator

from angelovich.core.DataStorage import Entity

from yap_torrent.components.peer_ec import (
	LocalUnchokedEC,
	PeerConnectionEC,
	PeerEC,
	PeerStatsEC,
	RemoteInterestedEC,
	RemoteUnchokedEC,
)
from yap_torrent.components.torrent_ec import TorrentEC
from yap_torrent.env import Env
from yap_torrent.protocol import bt_main_messages as msg
from yap_torrent.protocol.message import Message
from yap_torrent.system import TimeSystem
from yap_torrent.systems import get_info_hash, is_torrent_complete, iterate_connected_peers
from yap_torrent.systems.peer_logic import ChokeCandidate, select_unchoked

logger = logging.getLogger(__name__)

RECOMPUTE_INTERVAL = 30.0


class ChokeSystem(TimeSystem):
	_CHOKE_MESSAGES = (msg.MessageId.CHOKE.value, msg.MessageId.UNCHOKE.value)

	def __init__(self, env: Env):
		super().__init__(env, RECOMPUTE_INTERVAL)

	async def start(self):
		self.add_listener("peer.message", self.__on_message)
		self.add_listener("peer.connected", self.__on_peer_connected)
		self.add_listener("peer.disconnected", self._on_peer_disconnected)

	async def _update(self, delta_time: float):
		for torrent_entity in self.env.data_storage.get_collection(TorrentEC):
			await self._recompute(torrent_entity)

	async def __on_peer_connected(self, torrent_entity: Entity, peer_entity: Entity) -> None:
		# unchoke straight away if the upload queue still has room
		await self._recompute(torrent_entity)

	async def _on_peer_disconnected(self, _torrent_entity: Entity, peer_entity: Entity):
		for component in (LocalUnchokedEC, RemoteUnchokedEC):
			if peer_entity.has_component(component):
				peer_entity.remove_component(component)

	async def __on_message(self, torrent_entity: Entity, peer_entity: Entity, message: Message):
		if message.message_id not in self._CHOKE_MESSAGES:
			return
		message_id = msg.MessageId(message.message_id)
		# the remote peer is choking / unchoking US -> RemoteUnchokedEC marker
		if message_id == msg.MessageId.CHOKE:
			if peer_entity.has_component(RemoteUnchokedEC):
				peer_entity.remove_component(RemoteUnchokedEC)
			await self.env.event_bus.dispatch_async("peer.local.choked_changed", torrent_entity, peer_entity)
		elif message_id == msg.MessageId.UNCHOKE:
			if not peer_entity.has_component(RemoteUnchokedEC):
				peer_entity.add_component(RemoteUnchokedEC())
			await self.env.event_bus.dispatch_async("peer.local.choked_changed", torrent_entity, peer_entity)

	async def _recompute(self, torrent_entity: Entity):
		info_hash = get_info_hash(torrent_entity)
		seeding = is_torrent_complete(torrent_entity)
		limit = self.env.config.upload_peers_limit

		def candidate_iterator() -> Iterator[ChokeCandidate]:
			for e in iterate_connected_peers(self.env, info_hash):
				stats = e.get_component(PeerStatsEC)
				yield ChokeCandidate(
					key=e.get_component(PeerEC).key(),
					interested=e.has_component(RemoteInterestedEC),
					took=stats.uploaded,
					gave=stats.downloaded,
				)

		keep = select_unchoked(candidate_iterator(), limit, seeding)
		for peer_entity in iterate_connected_peers(self.env, info_hash):
			await self._set_unchoked(torrent_entity, peer_entity, peer_entity.get_component(PeerEC).key() in keep)

	async def _set_unchoked(self, torrent_entity: Entity, peer_entity: Entity, want: bool):
		if not peer_entity.has_component(PeerConnectionEC):
			return
		has = peer_entity.has_component(LocalUnchokedEC)
		if want and not has:
			peer_entity.add_component(LocalUnchokedEC())
			await peer_entity.get_component(PeerConnectionEC).send(msg.unchoke())
			await self.env.event_bus.dispatch_async("peer.remote.choked_changed", torrent_entity, peer_entity)
		elif has and not want:
			peer_entity.remove_component(LocalUnchokedEC)
			await peer_entity.get_component(PeerConnectionEC).send(msg.choke())
			await self.env.event_bus.dispatch_async("peer.remote.choked_changed", torrent_entity, peer_entity)
