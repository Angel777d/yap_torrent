import logging
from pathlib import Path
from typing import Optional

from angelovich.core.DataStorage import Entity

from yap_torrent.components.common import IdleEC
from yap_torrent.components.peer_ec import LocalUnchokedEC, PeerConnectionEC, PeerRateEC, PeerStatsEC
from yap_torrent.components.piece_ec import CompletePieceDataEC, PieceEC, UploadRequestedEC
from yap_torrent.components.torrent_ec import TorrentEC, TorrentInfoEC, TorrentState, TorrentStatsEC
from yap_torrent.env import Env
from yap_torrent.protocol import bt_main_messages as msg
from yap_torrent.protocol.message import Message
from yap_torrent.system import System
from yap_torrent.utils import check_hash, load_piece

logger = logging.getLogger(__name__)


class UploadSystem(System):
	_UPLOAD_MESSAGES = (msg.MessageId.REQUEST.value, msg.MessageId.CANCEL.value)

	async def start(self):
		self.add_listener("peer.message", self.__on_message)

	async def __on_message(self, torrent_entity: Entity, peer_entity: Entity, message: Message):
		if message.message_id not in self._UPLOAD_MESSAGES:
			return
		message_id = msg.MessageId(message.message_id)
		if message_id == msg.MessageId.REQUEST:
			await _process_request_message(self.env, peer_entity, torrent_entity, message)
		elif message_id == msg.MessageId.CANCEL:
			_process_cancel_message(self.env, peer_entity, torrent_entity, message)


def _find_or_load_piece(env: Env, torrent_entity: Entity, index: int) -> Optional[Entity]:
	ds = env.data_storage
	info_hash = torrent_entity.get_component(TorrentEC).info_hash
	torrent_info = torrent_entity.get_component(TorrentInfoEC).info

	piece_entity = ds.get_collection(PieceEC).find(PieceEC.make_hash(info_hash, index))
	if piece_entity:
		if piece_entity.has_component(CompletePieceDataEC):
			return piece_entity
		else:
			return None

	# lazily read from disk into a hash-verified CompletePieceDataEC + TTL
	root = Path(env.config.download_folder)
	data = load_piece(root, torrent_info, index)
	piece_info = torrent_info.get_piece_info(index)
	if not check_hash(data, piece_info.piece_hash):
		logger.error("Piece %s in %s is broken on request", index, torrent_info.name)
		return None

	return (ds.create_entity()
	        .add_component(PieceEC(info_hash, piece_info))
	        .add_component(CompletePieceDataEC(data))
	        .add_component(IdleEC()))


async def _process_request_message(env: Env, peer_entity: Entity, torrent_entity: Entity, message: Message):
	if torrent_entity.get_component(TorrentStatsEC).state == TorrentState.Inactive:
		return

	if not peer_entity.has_component(LocalUnchokedEC):
		return

	connection = peer_entity.get_component(PeerConnectionEC).connection
	index, begin, length = msg.payload_request(message)

	piece_entity = _find_or_load_piece(env, torrent_entity, index)
	if not piece_entity:
		logger.warning("Peer request a piece we don't have yet")
		return

	if not piece_entity.has_component(UploadRequestedEC):
		piece_entity.add_component(UploadRequestedEC())
	piece_entity.get_component(UploadRequestedEC).peers.add(peer_entity)
	piece_entity.get_component(IdleEC).touch()

	data = piece_entity.get_component(CompletePieceDataEC).get_block(begin, length)
	await connection.send(msg.piece(index, begin, data))

	peer_entity.get_component(PeerStatsEC).add_uploaded(length)
	peer_entity.get_component(PeerRateEC).add_uploaded(length)
	torrent_entity.get_component(TorrentStatsEC).update_uploaded(length)


def _process_cancel_message(env: Env, peer_entity: Entity, torrent_entity: Entity, message: Message):
	info_hash = torrent_entity.get_component(TorrentEC).info_hash
	index, begin, length = msg.payload_request(message)
	piece_entity = env.data_storage.get_collection(PieceEC).find(PieceEC.make_hash(info_hash, index))
	if piece_entity is not None and piece_entity.has_component(UploadRequestedEC):
		piece_entity.get_component(UploadRequestedEC).peers.discard(peer_entity)
