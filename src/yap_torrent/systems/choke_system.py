import logging
import time

from angelovich.core.DataStorage import Entity

from yap_torrent.components.peer_ec import (
	LocalUnchokedEC,
	PeerConnectionEC,
	PeerStatsEC,
	RemoteInterestedEC,
	RemoteUnchokedEC,
)
from yap_torrent.env import Env
from yap_torrent.protocol import bt_main_messages as msg
from yap_torrent.protocol.message import Message
from yap_torrent.system import System
from yap_torrent.systems import get_info_hash, get_torrent_entity, is_torrent_complete, iterate_peers
from yap_torrent.systems.peer_logic import ChokeCandidate, select_unchoked

logger = logging.getLogger(__name__)

RECOMPUTE_INTERVAL = 30.0


class ChokeSystem(System):
	_CHOKE_MESSAGES = (msg.MessageId.CHOKE.value, msg.MessageId.UNCHOKE.value)

	def __init__(self, env: Env):
		super().__init__(env)
		self._accum = 0.0

	async def start(self):
		self.add_listener("peer.message", self.__on_message)
		self.add_listener("peer.connected", self.__on_peer_connected)
		self.add_listener("action.torrent.stop", self._on_torrent_stop)

	async def _update(self, delta_time: float):
		self._accum += delta_time
		if self._accum < RECOMPUTE_INTERVAL:
			return
		self._accum = 0.0
		now = time.monotonic()
		from yap_torrent.components.torrent_ec import TorrentEC
		for torrent_entity in self.env.data_storage.get_collection(TorrentEC):
			await self._recompute(torrent_entity, now)

	async def __on_peer_connected(self, torrent_entity: Entity, peer_entity: Entity) -> None:
		# unchoke straight away if the upload queue still has room
		await self._recompute(torrent_entity, time.monotonic())

	async def _on_torrent_stop(self, torrent_entity: Entity):
		for peer_entity in list(iterate_peers(self.env, get_info_hash(torrent_entity))):
			await self._set_unchoked(torrent_entity, peer_entity, False)

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

	async def _recompute(self, torrent_entity: Entity, now: float):
		info_hash = get_info_hash(torrent_entity)
		seeding = is_torrent_complete(torrent_entity)
		limit = self.env.config.upload_peers_limit

		peers = list(iterate_peers(self.env, info_hash))
		candidates = []
		for peer_entity in peers:
			stats = peer_entity.get_component(PeerStatsEC)
			stats.sample_rate(now)
			candidates.append(ChokeCandidate(
				key=id(peer_entity),
				interested=peer_entity.has_component(RemoteInterestedEC),
				reciprocated=stats.downloaded > 0,  # they gave us data at least once
				rate=stats.up_rate,                  # our serving rate to them
			))

		keep = select_unchoked(candidates, limit, seeding)
		for peer_entity in peers:
			await self._set_unchoked(torrent_entity, peer_entity, id(peer_entity) in keep)

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
