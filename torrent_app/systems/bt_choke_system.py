import logging

from angelovichcore.DataStorage import Entity
from torrent_app import System
from torrent_app.components.peer_ec import PeerConnectionEC
from torrent_app.protocol import bt_main_messages as msg
from torrent_app.protocol.message import Message

logger = logging.getLogger(__name__)


class BTChokeSystem(System):
	_CHOKE_MESSAGES = (msg.MessageId.CHOKE.value, msg.MessageId.UNCHOKE.value)

	async def start(self):
		self.env.event_bus.add_listener("peer.message", self.__on_message, scope=self)
		self.env.event_bus.add_listener("peer.connected", self.__on_peer_connected, scope=self)

	async def __on_peer_connected(self, torrent_entity: Entity, peer_entity: Entity) -> None:
		await self.update_remote_choked(torrent_entity, peer_entity)

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
		self.env.event_bus.dispatch("peer.local.choked_changed", torrent_entity, peer_entity)

	async def update_remote_choked(self, torrent_entity: Entity, peer_entity: Entity):
		# TODO: fix choke algorythm
		# for simple start just unchoke any connected peer
		msg.unchoke()
		peer_connection_ec = peer_entity.get_component(PeerConnectionEC)
		peer_connection_ec.remote_choked = False
		self.env.event_bus.dispatch("peer.remote.choked_changed", torrent_entity, peer_entity)
