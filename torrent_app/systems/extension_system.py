import logging

from angelovichcore.DataStorage import Entity
from torrent_app import System
from torrent_app.components.extensions import PeerExtensionsEC, UT_METADATA, METADATA_PIECE_SIZE, TorrentMetadataEC
from torrent_app.components.peer_ec import PeerConnectionEC
from torrent_app.components.torrent_ec import TorrentHashEC, TorrentInfoEC
from torrent_app.protocol import encode, extensions, decode, TorrentInfo
from torrent_app.protocol.connection import Message, MessageId
from torrent_app.protocol.extensions import check_extension, extension_handshake
from torrent_app.utils import check_hash

logger = logging.getLogger(__name__)

# set supported here
PeerExtensionsEC.setup(
	UT_METADATA,
)


class ExtensionSystem(System):
	async def start(self) -> 'System':
		self.env.event_bus.add_listener("torrent.peer.ready", self.__on_peer_ready, scope=self)
		self.env.event_bus.add_listener("torrent.peer.message.extended", self.__on_message, scope=self)
		return await super().start()

	def close(self):
		self.env.event_bus.remove_all(scope=self)
		super().close()

	async def __on_message(self, torrent_entity: Entity, peer_entity: Entity, message: Message) -> None:
		metadata_ec = torrent_entity.get_component(TorrentMetadataEC)
		peer_connection_ec = peer_entity.get_component(PeerConnectionEC)
		peer_id = peer_connection_ec.connection.remote_peer_id

		ext_id, payload = message.extended

		# ext id = 0 is a handshake message
		if ext_id == 0:
			logger.info(f"Got extension handshake {payload} from peer {peer_id}")
			remote_ext_to_id = payload.get("m", {})
			peer_entity.add_component(PeerExtensionsEC(remote_ext_to_id))
			metadata_size = payload.get("metadata_size", -1)
			if metadata_size != -1 and not metadata_ec.is_complete():
				metadata_ec.metadata_size = metadata_size
			return

		ext_ec = peer_entity.get_component(PeerExtensionsEC)
		ext_name = ext_ec.LOCAL_ID_TO_EXT.get(ext_id, "")
		if ext_name == UT_METADATA:

			assert "msg_type" in payload
			assert "piece" in payload
			msg_type = payload["msg_type"]
			piece = payload["piece"]

			# 0 = request
			if msg_type == 0:
				# send piece
				if metadata_ec.is_complete():
					start = piece * METADATA_PIECE_SIZE
					full_amount = metadata_ec.metadata_size // METADATA_PIECE_SIZE
					size = metadata_ec.metadata_size % METADATA_PIECE_SIZE if piece == full_amount else METADATA_PIECE_SIZE
					data = metadata_ec.metadata[start:start + size]
					payload = encode({
						"msg_type": 1,  # data
						"piece": piece,
						"total_size": metadata_ec.metadata_size
					})
					remote_ext_id = ext_ec.remote_ext_to_id[UT_METADATA]
					await peer_connection_ec.connection.extended(remote_ext_id, payload + data)
				# send reject
				else:
					payload = encode({
						"msg_type": 2,  # reject
						"piece": piece,
					})
					remote_ext_id = ext_ec.remote_ext_to_id[UT_METADATA]
					await peer_connection_ec.connection.extended(remote_ext_id, payload)
			# 1 = data
			elif msg_type == 1:
				assert "total_size" in payload
				total_size = payload["total_size"]
				full_amount = metadata_ec.metadata_size // METADATA_PIECE_SIZE
				size = total_size % METADATA_PIECE_SIZE if piece == full_amount else METADATA_PIECE_SIZE
				data = message.raw_payload[-size:]
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

			# 2 = reject
			elif msg_type == 2:
				# TODO: ignore this peer for a while
				pass
			else:
				raise RuntimeError(f"Unknown message type {msg_type} for UT_METADATA protocol extension")

	async def __on_peer_ready(self, torrent_entity: Entity, peer_entity: Entity) -> None:
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

		# add metadata info to handshake
		if torrent_entity.get_component(TorrentMetadataEC).is_complete():
			metadata_ec = torrent_entity.get_component(TorrentMetadataEC)
			additional_fields["metadata_size"] = metadata_ec.metadata_size

		handshake = extension_handshake(PeerExtensionsEC.EXT_TO_ID, **additional_fields)
		await peer_connection_ec.connection.extended(0, handshake)
