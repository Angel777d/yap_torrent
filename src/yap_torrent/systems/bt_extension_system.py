import asyncio
import logging

from angelovich.core.DataStorage import Entity

from yap_torrent.components.extensions import PeerExtensionsEC
from yap_torrent.components.peer_ec import PeerConnectionEC
from yap_torrent.protocol import bt_ext_messages as msg
from yap_torrent.protocol import extensions
from yap_torrent.protocol.connection import Message
from yap_torrent.protocol.extensions import check_extension, extension_handshake
from yap_torrent.system import System

logger = logging.getLogger(__name__)


class BTExtensionSystem(System):
	async def start(self):
		self.env.event_bus.add_listener("peer.connected", self.__on_peer_connected, scope=self)
		self.env.event_bus.add_listener("peer.message", self.__on_message, scope=self)

	def close(self):
		self.env.event_bus.remove_all_listeners(scope=self)
		super().close()

	async def __on_message(self, torrent_entity: Entity, peer_entity: Entity, message: Message) -> None:
		if message.message_id != msg.EXTENDED:
			return

		ext_id, payload = msg.payload_extended(message)
		peer_connection_ec = peer_entity.get_component(PeerConnectionEC)

		# ext id = 0 is a handshake message
		if ext_id == 0:
			peer_id = peer_connection_ec.connection.remote_peer_id
			logger.debug(f"Got extension handshake {payload} from peer {peer_id}")

			remote_ext_to_id = payload.get("m", {})
			peer_entity.add_component(PeerExtensionsEC(remote_ext_to_id))
			self.env.event_bus.dispatch("protocol.extensions.got_handshake", torrent_entity, peer_entity, payload)
		else:
			# check is a metadata message
			ext_ec = peer_entity.get_component(PeerExtensionsEC)
			ext_name = ext_ec.get_extension_name(ext_id)
			logger.info(f"Got extension message {ext_name} from peer {peer_connection_ec.connection.remote_peer_id}")
			self.env.event_bus.dispatch(f"protocol.extensions.message.{ext_name}", torrent_entity, peer_entity, message)

	async def __on_peer_connected(self, torrent_entity: Entity, peer_entity: Entity) -> None:
		peer_connection_ec = peer_entity.get_component(PeerConnectionEC)
		reserved = peer_connection_ec.reserved
		if not check_extension(reserved, extensions.EXTENSION_PROTOCOL):
			return

		# https://www.bittorrent.org/beps/bep_0010.html
		additional_fields = {
			"p": self.env.config.port,
			"v": "Another Python Torrent 0.0.1",
			"yourip": None,  # TODO: add address
			"ipv6": None,
			"ipv4": None,
			"reqq": 250,  # TODO: check what is it
		}

		# some extensions write data to the handshake message as well
		tasks = self.env.event_bus.dispatch(
			"protocol.extensions.create_handshake",
			torrent_entity=torrent_entity,
			additional_fields=additional_fields)
		await asyncio.gather(*tasks)

		handshake = extension_handshake(PeerExtensionsEC.EXT_TO_ID, **additional_fields)
		await peer_connection_ec.connection.send(msg.extended(0, handshake))
