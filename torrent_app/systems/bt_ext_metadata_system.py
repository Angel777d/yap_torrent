import logging
from typing import Any, Dict

from angelovichcore.DataStorage import Entity
from torrent_app import System
from torrent_app.components.extensions import TorrentMetadataEC, PeerExtensionsEC, UT_METADATA, METADATA_PIECE_SIZE
from torrent_app.components.peer_ec import PeerConnectionEC
from torrent_app.components.torrent_ec import TorrentHashEC, TorrentInfoEC
from torrent_app.protocol import bt_ext_messages as msg
from torrent_app.protocol import encode, decode, TorrentInfo
from torrent_app.protocol.connection import Message
from torrent_app.utils import check_hash

logger = logging.getLogger(__name__)


class BTExtMetadataSystem(System):
	async def start(self):
		PeerExtensionsEC.add_supported(UT_METADATA)

		self.env.event_bus.add_listener(f"protocol.extensions.message.{UT_METADATA}", self.__on_ext_message, scope=self)
		self.env.event_bus.add_listener("protocol.extensions.create_handshake", self.__on_create_handshake, scope=self)
		self.env.event_bus.add_listener("protocol.extensions.got_handshake", self.__on_got_handshake, scope=self)

	async def __on_create_handshake(self, torrent_entity: Entity, additional_fields: dict[str, Any]) -> None:
		if not torrent_entity.has_component(TorrentInfoEC):
			return
		additional_fields["metadata_size"] = len(torrent_entity.get_component(TorrentInfoEC).info.get_metadata())

	async def __on_got_handshake(self, torrent_entity: Entity, payload: Dict[str, Any]) -> None:
		metadata_size = payload.get("metadata_size", -1)

		metadata_ec = TorrentMetadataEC()
		# fill local metadata if possible
		if torrent_entity.has_component(TorrentInfoEC):
			metadata = torrent_entity.get_component(TorrentInfoEC).info.get_metadata()
			metadata_ec.set_metadata(metadata)
		# use metadata from handshake if any
		elif metadata_size > 0:
			metadata_ec.metadata_size = metadata_size
		# early exit in case peer don't have metadata info
		else:
			return

		torrent_entity.add_component(metadata_ec)

	async def __on_ext_message(self, torrent_entity: Entity, peer_entity: Entity, message: Message) -> None:
		ext_id, payload = msg.payload_extended(message)
		ext_ec = peer_entity.get_component(PeerExtensionsEC)

		# to continue process we need metadata_ec created on handshake
		if not torrent_entity.has_component(TorrentMetadataEC):
			logging.error("TorrentMetadataEC not found")
			return

		peer_connection_ec = peer_entity.get_component(PeerConnectionEC)
		metadata_ec = torrent_entity.get_component(TorrentMetadataEC)

		if "msg_type" not in payload:
			raise RuntimeError("msg_type not found in payload")
		msg_type = payload["msg_type"]

		if "piece" not in payload:
			raise RuntimeError("piece not found in payload")
		piece = payload["piece"]

		if msg_type == 0:  # request
			# send piece
			if metadata_ec.is_complete():
				start = piece * METADATA_PIECE_SIZE
				full_amount = metadata_ec.metadata_size // METADATA_PIECE_SIZE
				size = metadata_ec.metadata_size % METADATA_PIECE_SIZE if piece == full_amount else METADATA_PIECE_SIZE
				data = metadata_ec.metadata[start:start + size]
				ext_message = encode({
					"msg_type": 1,  # data
					"piece": piece,
					"total_size": metadata_ec.metadata_size
				})
				remote_ext_id = ext_ec.remote_ext_to_id[UT_METADATA]
				await peer_connection_ec.connection.send(msg.extended(remote_ext_id, ext_message + data))
			# send reject
			else:
				ext_message = encode({
					"msg_type": 2,  # reject
					"piece": piece,
				})
				remote_ext_id = ext_ec.remote_ext_to_id[UT_METADATA]
				await peer_connection_ec.connection.send(msg.extended(remote_ext_id, ext_message))
		elif msg_type == 1:  # data
			if "total_size" not in payload:
				raise RuntimeError("total_size not found in payload")

			total_size = payload["total_size"]
			full_amount = metadata_ec.metadata_size // METADATA_PIECE_SIZE
			size = total_size % METADATA_PIECE_SIZE if piece == full_amount else METADATA_PIECE_SIZE
			data = message.payload[-size:]
			metadata_ec.add_piece(piece, data)
			downloaded = sum(len(i) for i in metadata_ec.pieces.values())
			if downloaded == total_size:
				metadata = bytearray()
				for k, v in sorted(((k, v) for k, v in metadata_ec.pieces.items()), key=lambda i: i[0]):
					metadata.extend(v)
				metadata = bytes(metadata)
				info_hash = torrent_entity.get_component(TorrentHashEC).info_hash
				if check_hash(info_hash, metadata):
					metadata_ec.set_metadata(metadata)
					data = decode(metadata)
					torrent_info = TorrentInfo(data)
					torrent_entity.add_component(TorrentInfoEC(torrent_info))
					# TODO: update interested to connected peers and start download torrent
					logger.info(f"Successfully loaded metadata for torrent {torrent_info.name}")
				else:
					metadata_ec.pieces.clear()
					logger.info(f"Failed to load proper metadata for torrent {info_hash}")

		elif msg_type == 2:  # reject
			# TODO: ignore this peer for a while
			pass
		else:
			raise RuntimeError(f"Unknown message type {msg_type} for UT_METADATA protocol extension")
