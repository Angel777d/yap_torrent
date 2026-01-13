import asyncio
import logging

from angelovich.core.DataStorage import Entity

from yap_torrent.components.peer_ec import PeerConnectionEC
from yap_torrent.env import Env
from yap_torrent.protocol import bt_main_messages as msg
from yap_torrent.protocol.message import Message
from yap_torrent.system import System
from yap_torrent.systems import get_torrent_entity, iterate_peers

logger = logging.getLogger(__name__)


class BTChokeSystem(System):
	_CHOKE_MESSAGES = (msg.MessageId.CHOKE.value, msg.MessageId.UNCHOKE.value)

	async def start(self):
		self.env.event_bus.add_listener("peer.message", self.__on_message, scope=self)
		self.env.event_bus.add_listener("peer.connected", self.__on_peer_connected, scope=self)
		self.env.event_bus.add_listener("action.torrent.stop", self._on_torrent_stop, scope=self)

	async def _on_torrent_stop(self, info_hash: bytes):
		torrent_entity = get_torrent_entity(self.env, info_hash)
		tasks = [_update_remote_choked(self.env, torrent_entity, peer_entity, True)
		         for peer_entity in iterate_peers(self.env, info_hash)]
		await asyncio.gather(*tasks)

	async def __on_peer_connected(self, torrent_entity: Entity, peer_entity: Entity) -> None:
		# TODO: implement choke algorythm
		await _update_remote_choked(self.env, torrent_entity, peer_entity, False)

	async def __on_message(self, torrent_entity: Entity, peer_entity: Entity, message: Message):
		if message.message_id not in self._CHOKE_MESSAGES:
			return

		message_id = msg.MessageId(message.message_id)

		if message_id == msg.MessageId.CHOKE:
			logger.debug("%s choked us", peer_entity.get_component(PeerConnectionEC))
			await self.update_local_choked(torrent_entity, peer_entity, True)
		elif message_id == msg.MessageId.UNCHOKE:
			logger.debug("%s unchoked us", peer_entity.get_component(PeerConnectionEC))
			await self.update_local_choked(torrent_entity, peer_entity, False)

	async def update_local_choked(self, torrent_entity: Entity, peer_entity: Entity, new_value: bool):
		peer_connection_ec = peer_entity.get_component(PeerConnectionEC)
		if peer_connection_ec.local_choked == new_value:
			return

		peer_connection_ec.local_choked = new_value
		await asyncio.gather(*self.env.event_bus.dispatch("peer.local.choked_changed", torrent_entity, peer_entity))


async def _update_remote_choked(env: Env, torrent_entity: Entity, peer_entity: Entity, new_choked: bool):
	peer_connection_ec = peer_entity.get_component(PeerConnectionEC)
	if peer_connection_ec.remote_choked == new_choked:
		return

	peer_connection_ec.remote_choked = new_choked
	if new_choked:
		await peer_connection_ec.connection.send(msg.choke())
	else:
		await peer_connection_ec.connection.send(msg.unchoke())
	env.event_bus.dispatch("peer.remote.choked_changed", torrent_entity, peer_entity)
