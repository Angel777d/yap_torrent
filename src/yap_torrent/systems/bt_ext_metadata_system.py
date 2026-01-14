import logging
from typing import Any, Dict

from angelovich.core.DataStorage import Entity

from yap_torrent.components.extensions import TorrentMetadataEC, PeerExtensionsEC, UT_METADATA, METADATA_PIECE_SIZE
from yap_torrent.components.peer_ec import PeerConnectionEC
from yap_torrent.components.torrent_ec import TorrentEC, TorrentInfoEC
from yap_torrent.protocol import bt_ext_messages as msg
from yap_torrent.protocol import encode, decode, TorrentInfo
from yap_torrent.protocol.connection import Message
from yap_torrent.system import System
from yap_torrent.utils import check_hash

logger = logging.getLogger(__name__)


class BTExtMetadataSystem(System):
	async def start(self):
		PeerExtensionsEC.add_supported(UT_METADATA)

		self.env.event_bus.add_listener(f"protocol.extensions.message.{UT_METADATA}", self.__on_ext_message, scope=self)
		self.env.event_bus.add_listener("protocol.extensions.create_handshake", self.__on_create_handshake, scope=self)
		self.env.event_bus.add_listener("protocol.extensions.got_handshake", self.__on_got_handshake, scope=self)

		collection = self.env.data_storage.get_collection(TorrentEC)
		collection.add_listener(collection.EVENT_ADDED, self.__on_torrent_added, self)
		for entity in collection.entities:
			entity.add_component(TorrentMetadataEC())

		return await super().start()

	def close(self):
		self.env.event_bus.remove_all_listeners(scope=self)

		collection = self.env.data_storage.get_collection(TorrentEC)
		collection.remove_all_listeners(self)
		super().close()

	async def __on_torrent_added(self, entity: Entity, component: TorrentEC):
		entity.add_component(TorrentMetadataEC())

	async def __on_create_handshake(self, torrent_entity: Entity, additional_fields: dict[str, Any]) -> None:
		additional_fields["metadata_size"] = 0
		if torrent_entity.has_component(TorrentInfoEC):
			additional_fields["metadata_size"] = len(torrent_entity.get_component(TorrentInfoEC).info.get_metadata())

	async def __on_got_handshake(self, torrent_entity: Entity, peer_entity: Entity, payload: Dict[str, Any]) -> None:
		metadata_size = payload.get("metadata_size", -1)
		metadata_ec = torrent_entity.get_component(TorrentMetadataEC)

		# fill local metadata if possible
		if torrent_entity.has_component(TorrentInfoEC):
			metadata = torrent_entity.get_component(TorrentInfoEC).info.get_metadata()
			metadata_ec.set_metadata(metadata)
		# use metadata from handshake if any
		elif metadata_size > 0:
			metadata_ec.metadata_size = metadata_size
		# early exit in case the peer doesn't have metadata info
		else:
			return

		# just wait for a metadata request
		if metadata_ec.is_complete():
			return

		ext_ec = peer_entity.get_component(PeerExtensionsEC)
		remote_ext_id = ext_ec.remote_ext_to_id[UT_METADATA]
		peer_connection_ec = peer_entity.get_component(PeerConnectionEC)

		info_hash = torrent_entity.get_component(TorrentEC).info_hash
		logger.info(f"Start metadata load for torrent [{info_hash}]")

		ext_message = encode({"msg_type": 0, "piece": 0})
		message = msg.extended(remote_ext_id, ext_message)
		await peer_connection_ec.connection.send(message)

	async def __on_ext_message(self, torrent_entity: Entity, peer_entity: Entity, message: Message) -> None:
		ext_id, payload = msg.payload_extended(message)

		ext_ec = peer_entity.get_component(PeerExtensionsEC)
		remote_ext_id = ext_ec.remote_ext_to_id[UT_METADATA]

		# to continue the process, we need metadata_ec
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
			# send a piece
			if metadata_ec.is_complete():
				start = piece * METADATA_PIECE_SIZE
				last_piece = metadata_ec.metadata_size // METADATA_PIECE_SIZE
				size = metadata_ec.metadata_size % METADATA_PIECE_SIZE if piece == last_piece else METADATA_PIECE_SIZE
				data = metadata_ec.metadata[start:start + size]
				ext_message = encode({
					"msg_type": 1,  # data
					"piece": piece,
					"total_size": metadata_ec.metadata_size
				})
				await peer_connection_ec.connection.send(msg.extended(remote_ext_id, ext_message + data))
			# send reject
			else:
				ext_message = encode({
					"msg_type": 2,  # reject
					"piece": piece,
				})
				await peer_connection_ec.connection.send(msg.extended(remote_ext_id, ext_message))
		elif msg_type == 1:  # data
			if "total_size" not in payload:
				raise RuntimeError("total_size not found in payload")

			# already downloaded all pieces
			if metadata_ec.is_complete():
				return

			total_size = payload["total_size"]
			last_piece = metadata_ec.metadata_size // METADATA_PIECE_SIZE
			size = total_size % METADATA_PIECE_SIZE if piece == last_piece else METADATA_PIECE_SIZE
			data = message.payload[-size:]
			metadata_ec.add_piece(piece, data)
			downloaded = sum(len(i) for i in metadata_ec.pieces.values())

			# metadata download completed
			logger.info(f"Metadata download progress {downloaded} {total_size}")
			if downloaded == total_size:
				metadata = bytearray()
				for i in range(len(metadata_ec.pieces)):
					metadata.extend(metadata_ec.pieces[i])
				metadata = bytes(metadata)
				info_hash = torrent_entity.get_component(TorrentEC).info_hash
				if check_hash(metadata, info_hash):
					metadata_ec.set_metadata(metadata)
					torrent_info = TorrentInfo(decode(metadata))
					torrent_entity.add_component(TorrentInfoEC(torrent_info))

					# disconnect all peers and start validation
					self.env.event_bus.dispatch("request.torrent.invalidate", info_hash)
					logger.info(f"Successfully loaded metadata for torrent {torrent_info.name}")
				else:
					metadata_ec.pieces.clear()
					logger.info(f"Failed to load proper metadata for torrent {info_hash}")

					# start from the beginning
					ext_message = encode({"msg_type": 0, "piece": 0})
					await peer_connection_ec.connection.send(msg.extended(remote_ext_id, ext_message))

			# load next piece
			else:
				ext_message = encode({
					"msg_type": 0,  # request
					"piece": piece + 1,
				})
				await peer_connection_ec.connection.send(msg.extended(remote_ext_id, ext_message))

		elif msg_type == 2:  # reject
			# TODO: ignore this peer for a while
			pass
		else:
			raise RuntimeError(f"Unknown message type {msg_type} for UT_METADATA protocol extension")
