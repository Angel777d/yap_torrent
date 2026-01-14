import logging
from pathlib import Path

from angelovich.core.DataStorage import Entity

from yap_torrent.components.peer_ec import PeerConnectionEC
from yap_torrent.components.piece_ec import PieceEC, PiecePendingRemoveEC
from yap_torrent.components.torrent_ec import TorrentEC, TorrentInfoEC, TorrentStatsEC
from yap_torrent.env import Env
from yap_torrent.protocol import bt_main_messages as msg
from yap_torrent.protocol.message import Message
from yap_torrent.system import System
from yap_torrent.utils import load_piece, check_hash

logger = logging.getLogger(__name__)


class BTUploadSystem(System):
	_UPLOAD_MESSAGES = (msg.MessageId.REQUEST.value, msg.MessageId.CANCEL.value)

	async def start(self):
		self.env.event_bus.add_listener("peer.message", self.__on_message, scope=self)
		self.env.event_bus.add_listener("peer.remote.interested_changed", self.__on_remote_peer_changed, scope=self)
		self.env.event_bus.add_listener("peer.remote.choked_changed", self.__on_remote_peer_changed, scope=self)

	async def __on_remote_peer_changed(self, torrent_entity: Entity, peer_entity: Entity) -> None:
		pass

	async def __on_message(self, torrent_entity: Entity, peer_entity: Entity, message: Message):
		if message.message_id not in self._UPLOAD_MESSAGES:
			return

		message_id = msg.MessageId(message.message_id)
		if message_id == msg.MessageId.REQUEST:
			await _process_request_message(self.env, peer_entity, torrent_entity, message)
		elif message_id == msg.MessageId.CANCEL:
			pass


async def _process_request_message(env: Env, peer_entity: Entity, torrent_entity: Entity, message: Message):
	ds = env.data_storage
	config = env.config
	info_hash = torrent_entity.get_component(TorrentEC).info_hash
	torrent_info = torrent_entity.get_component(TorrentInfoEC).info
	connection = peer_entity.get_component(PeerConnectionEC).connection

	index, begin, length = msg.payload_request(message)

	piece_entity = ds.get_collection(PieceEC).find(PieceEC.make_hash(info_hash, index))

	# load piece
	if not piece_entity:
		root = Path(config.download_folder)
		data = load_piece(root, torrent_info, index)

		piece_info = torrent_entity.get_component(TorrentInfoEC).info.get_piece_info(index)
		piece_ec = PieceEC(info_hash, piece_info)
		piece_ec.set_data(data)
		piece_entity = ds.create_entity().add_component(piece_ec)
		piece_entity.add_component(PiecePendingRemoveEC())

	piece_ec = piece_entity.get_component(PieceEC)
	if not piece_ec.completed:
		logger.error(f"Piece {index} in {torrent_info.name} is not completed on request")
		# TODO: how did we get here?
		return

	if not check_hash(piece_ec.data, torrent_info.get_piece_hash(index)):
		logger.error(f"Piece {index} in {torrent_info.name} torrent is broken")
		# TODO: check files, reload piece
		return

	data = piece_ec.get_block(begin, length)
	piece_entity.get_component(PiecePendingRemoveEC).update()

	await connection.send(msg.piece(index, begin, data))
	torrent_entity.get_component(TorrentStatsEC).update_uploaded(length)
