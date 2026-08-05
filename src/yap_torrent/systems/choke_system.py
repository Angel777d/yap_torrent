import logging
from typing import Iterator, List

from angelovich.core.DataStorage import Entity
from angelovich.core.System import System, TimeSystem

from yap_torrent.components.peer_ec import (
	LocalUnchokedEC,
	PeerConnectionEC,
	PeerDisconnectedEC,
	PeerEC,
	PeerStatsEC,
	RemoteInterestedEC,
	RemoteUnchokedEC,
)
from yap_torrent.env import Env
from yap_torrent.protocol import bt_main_messages as msg
from yap_torrent.protocol.message import Message
from yap_torrent.systems import (
	get_info_hash,
	is_torrent_complete,
	iterate_connected_peers,
	iterate_torrents_in_queue_order,
)
from yap_torrent.systems.logic.peer import ChokeCandidate, select_unchoked

logger = logging.getLogger(__name__)

RECOMPUTE_INTERVAL = 30.0


def _candidates(peers: List[Entity]) -> Iterator[ChokeCandidate]:
	for entity in peers:
		stats = entity.get_component(PeerStatsEC)
		yield ChokeCandidate(
			key=entity.get_component(PeerEC).key(),
			interested=entity.has_component(RemoteInterestedEC),
			took=stats.uploaded,
			gave=stats.downloaded,
		)


class ChokeSystem(TimeSystem, System):
	_CHOKE_MESSAGES = (msg.MessageId.CHOKE.value, msg.MessageId.UNCHOKE.value)

	def __init__(self, env: Env):
		System.__init__(self, env)
		TimeSystem.__init__(self, RECOMPUTE_INTERVAL)

	async def start(self):
		self.add_listener("peer.message", self.__on_message)
		self.add_listener("peer.remote.interested_changed", self._recompute)
		self.add_listener("peer.connected", self._recompute)
		self.add_listener("peer.disconnected", self._on_peer_disconnected)

	async def _update(self, delta_time: float):
		await self._recompute()

	async def _on_peer_disconnected(self, _torrent_entity: Entity, peer_entity: Entity):
		for component in (LocalUnchokedEC, RemoteUnchokedEC):
			if peer_entity.has_component(component):
				peer_entity.remove_component(component)

		await self._recompute()

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

	async def _recompute(self, *args, **kwargs):
		remaining = self.env.config.upload_peers_limit

		for torrent_entity in iterate_torrents_in_queue_order(self.env):
			peers = [e for e in iterate_connected_peers(self.env, get_info_hash(torrent_entity))
			         if not e.has_component(PeerDisconnectedEC)]
			if not peers:
				continue

			contenders = [e for e in peers if e.has_component(RemoteInterestedEC)]

			keep = select_unchoked(_candidates(contenders), remaining, is_torrent_complete(torrent_entity))
			for peer_entity in peers:
				await self._set_unchoked(torrent_entity, peer_entity,
				                         peer_entity.get_component(PeerEC).key() in keep)
			remaining -= len(keep)

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
