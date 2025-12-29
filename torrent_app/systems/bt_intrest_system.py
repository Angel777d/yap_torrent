import logging

from angelovichcore.DataStorage import Entity
from torrent_app import System
from torrent_app.components.bitfield_ec import BitfieldEC
from torrent_app.components.peer_ec import PeerConnectionEC, PeerInfoEC
from torrent_app.components.piece_ec import PieceEC
from torrent_app.components.torrent_ec import TorrentHashEC
from torrent_app.protocol import bt_main_messages as msg
from torrent_app.protocol.message import Message

logger = logging.getLogger(__name__)


class BTInterestedSystem(System):
	_INTERESTED_MESSAGES = (
		msg.MessageId.INTERESTED.value,
		msg.MessageId.NOT_INTERESTED.value,
		msg.MessageId.HAVE.value,
		msg.MessageId.BITFIELD.value
	)

	async def start(self):
		self.env.event_bus.add_listener("peer.message", self.__on_message, scope=self)
		self.env.event_bus.add_listener("piece.complete", self.__on_piece_complete, scope=self)
		self.env.event_bus.add_listener("peer.connected", self.__on_peer_connected, scope=self)

	async def __on_peer_connected(self, torrent_entity: Entity, peer_entity: Entity) -> None:
		await self.update_local_interested(torrent_entity, peer_entity)

	async def __on_piece_complete(self, torrent_entity: Entity, piece_entity: Entity):
		info_hash = torrent_entity.get_component(TorrentHashEC).info_hash
		index = piece_entity.get_component(PieceEC).info.index

		# notify all
		peers_collection = self.env.data_storage.get_collection(PeerConnectionEC).entities
		for peer_entity in peers_collection:
			if peer_entity.get_component(PeerInfoEC).info_hash == info_hash:
				await peer_entity.get_component(PeerConnectionEC).connection.send(msg.have(index))
				await self.update_local_interested(torrent_entity, peer_entity)

	async def __on_message(self, torrent_entity: Entity, peer_entity: Entity, message: Message):
		if message.message_id not in self._INTERESTED_MESSAGES:
			return

		bitfield_ec = peer_entity.get_component(BitfieldEC)
		message_id = msg.MessageId(message.message_id)

		if message_id == msg.MessageId.HAVE:
			bitfield_ec.set_index(msg.payload_index(message))
			await self.update_local_interested(torrent_entity, peer_entity)
		elif message_id == msg.MessageId.BITFIELD:
			bitfield_ec.update(msg.payload_bitfield(message))
			await self.update_local_interested(torrent_entity, peer_entity)
		elif message_id == msg.MessageId.INTERESTED:
			await self.update_remote_interested(torrent_entity, peer_entity, True)
		elif message_id == msg.MessageId.NOT_INTERESTED:
			await self.update_remote_interested(torrent_entity, peer_entity, False)

	async def update_remote_interested(self, torrent_entity: Entity, peer_entity: Entity, new_value: bool):
		peer_connection_ec = peer_entity.get_component(PeerConnectionEC)
		if peer_connection_ec.local_interested == new_value:
			return

		peer_connection_ec.remote_interested = new_value
		self.env.event_bus.dispatch("peer.remote.interested_changed", torrent_entity, peer_entity)

	async def update_local_interested(self, torrent_entity: Entity, peer_entity: Entity):
		remote_bitfield = peer_entity.get_component(BitfieldEC)
		local_bitfield = torrent_entity.get_component(BitfieldEC)
		peer_connection_ec = peer_entity.get_component(PeerConnectionEC)

		old_interested = peer_connection_ec.local_interested
		if local_bitfield.interested_in(remote_bitfield):
			await peer_connection_ec.interested()
		else:
			await peer_connection_ec.not_interested()

		if peer_connection_ec.local_interested != old_interested:
			self.env.event_bus.dispatch("peer.local.interested_changed", torrent_entity, peer_entity)
