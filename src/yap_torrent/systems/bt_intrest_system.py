import asyncio
import logging

from angelovich.core.DataStorage import Entity

from yap_torrent.components.peer_ec import PeerConnectionEC
from yap_torrent.components.piece_ec import PieceEC
from yap_torrent.components.torrent_ec import TorrentEC
from yap_torrent.env import Env
from yap_torrent.protocol import bt_main_messages as msg
from yap_torrent.protocol.message import Message
from yap_torrent.system import System
from yap_torrent.systems import iterate_peers, get_torrent_entity

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
		self.env.event_bus.add_listener("action.torrent.stop", self._on_torrent_stop, scope=self)

	async def _on_torrent_stop(self, info_hash: bytes):
		torrent_entity = get_torrent_entity(self.env, info_hash)
		tasks = [_update_local_peer_interested(self.env, torrent_entity, peer_entity, False)
		         for peer_entity in iterate_peers(self.env, info_hash)]
		await asyncio.gather(*tasks)

	async def __on_peer_connected(self, torrent_entity: Entity, peer_entity: Entity) -> None:
		await self.update_local_interested(torrent_entity, peer_entity)

	async def __on_piece_complete(self, torrent_entity: Entity, piece_entity: Entity):
		info_hash = torrent_entity.get_component(TorrentEC).info_hash
		index = piece_entity.get_component(PieceEC).info.index

		# notify all
		peers_collection = self.env.data_storage.get_collection(PeerConnectionEC).entities
		for peer_entity in peers_collection:
			if peer_entity.get_component(PeerConnectionEC).info_hash == info_hash:
				await peer_entity.get_component(PeerConnectionEC).connection.send(msg.have(index))
				await self.update_local_interested(torrent_entity, peer_entity)

	async def __on_message(self, torrent_entity: Entity, peer_entity: Entity, message: Message):
		if message.message_id not in self._INTERESTED_MESSAGES:
			return

		bitfield = peer_entity.get_component(PeerConnectionEC).remote_bitfield
		message_id = msg.MessageId(message.message_id)

		if message_id == msg.MessageId.HAVE:
			bitfield.set_index(msg.payload_index(message))
			await self.update_local_interested(torrent_entity, peer_entity)
		elif message_id == msg.MessageId.BITFIELD:
			bitfield.update(msg.payload_bitfield(message))
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
		remote_bitfield = peer_entity.get_component(PeerConnectionEC).remote_bitfield
		local_bitfield = torrent_entity.get_component(TorrentEC).bitfield
		new_interested = local_bitfield.interested_in(remote_bitfield)
		await _update_local_peer_interested(self.env, torrent_entity, peer_entity, len(new_interested) > 0)


async def _update_local_peer_interested(env: Env, torrent_entity: Entity, peer_entity: Entity, new_interested: bool):
	peer_connection_ec = peer_entity.get_component(PeerConnectionEC)
	if peer_connection_ec.local_interested == new_interested:
		return

	logger.debug(f"Interested in: %s", peer_connection_ec)
	peer_connection_ec.local_interested = new_interested
	if new_interested:
		await peer_connection_ec.connection.send(msg.interested())
	else:
		await peer_connection_ec.connection.send(msg.not_interested())

	await asyncio.gather(*env.event_bus.dispatch("peer.local.interested_changed", torrent_entity, peer_entity))
