import logging
from typing import Set

from angelovich.core.DataStorage import Entity

from yap_torrent.components.peer_ec import (
	LocalInterestedEC,
	PeerConnectionEC,
	PeerDisconnectedEC,
	PeerEC,
	RemoteInterestedEC,
)
from yap_torrent.components.piece_ec import PieceEC
from yap_torrent.components.torrent_ec import TorrentDownloadProgressEC, TorrentEC, TorrentInfoEC
from yap_torrent.protocol import bt_main_messages as msg
from yap_torrent.protocol.message import Message
from yap_torrent.protocol.structures import Bitfield
from yap_torrent.system import System
from yap_torrent.systems import get_info_hash, iterate_connected_peers, iterate_torrents_in_queue_order

logger = logging.getLogger(__name__)


def interested_pieces(torrent_entity: Entity, remote_bitfield: Bitfield) -> Set[int]:
	missing = torrent_entity.get_component(TorrentEC).bitfield.interested_in(remote_bitfield)
	if torrent_entity.has_component(TorrentDownloadProgressEC):
		return missing.intersection(torrent_entity.get_component(TorrentDownloadProgressEC).wanted.have)
	return missing


class InterestedSystem(System):
	_INTERESTED_MESSAGES = (msg.MessageId.INTERESTED.value, msg.MessageId.NOT_INTERESTED.value,
	                        msg.MessageId.HAVE.value, msg.MessageId.BITFIELD.value)

	async def start(self):
		self.add_listener("peer.message", self.__on_message)
		self.add_listener("piece.complete", self.__on_piece_complete)
		self.add_listener("peer.connected", self.__on_peer_connected)
		self.add_listener("peer.disconnected", self.__on_peer_disconnected)
		self.add_listener("action.torrent.files_changed", self.__on_files_changed)

	async def __on_files_changed(self, torrent_entity: Entity):
		"""The wanted mask moved; re-derive interest across the torrent's peers."""
		for peer_entity in list(iterate_connected_peers(self.env, get_info_hash(torrent_entity))):
			await self.__update_local_interested(torrent_entity, peer_entity)
		await self.__refill_download_slots()

	async def __on_peer_connected(self, torrent_entity: Entity, peer_entity: Entity):
		await self.__update_local_interested(torrent_entity, peer_entity)

	async def __on_peer_disconnected(self, _torrent_entity: Entity, peer_entity: Entity):
		for component in (LocalInterestedEC, RemoteInterestedEC):
			if peer_entity.has_component(component):
				peer_entity.remove_component(component)

		await self.__refill_download_slots()

	async def __on_piece_complete(self, torrent_entity: Entity, piece_entity: Entity):
		info_hash = get_info_hash(torrent_entity)
		index = piece_entity.get_component(PieceEC).info.index

		for peer_entity in iterate_connected_peers(self.env, info_hash):
			await peer_entity.get_component(PeerConnectionEC).send(msg.have(index))
			await self.__update_local_interested(torrent_entity, peer_entity)

		await self.__refill_download_slots()

	async def __refill_download_slots(self):
		limit = self.env.config.download_peers_limit
		if self._interested_count() >= limit:
			return

		for torrent_entity in iterate_torrents_in_queue_order(self.env):
			if not torrent_entity.has_component(TorrentInfoEC):
				continue
			for peer_entity in list(iterate_connected_peers(self.env, get_info_hash(torrent_entity))):
				if peer_entity.has_component(LocalInterestedEC) or peer_entity.has_component(PeerDisconnectedEC):
					continue
				await self.__update_local_interested(torrent_entity, peer_entity)
				if self._interested_count() >= limit:
					return

	async def __on_message(self, torrent_entity: Entity, peer_entity: Entity, message: Message):
		if message.message_id not in self._INTERESTED_MESSAGES:
			return

		peer_ec = peer_entity.get_component(PeerEC)
		remote_bitfield = peer_ec.remote_bitfield
		message_id = msg.MessageId(message.message_id)

		if message_id == msg.MessageId.HAVE:
			remote_bitfield.set_index(msg.payload_index(message))
			await self.__update_local_interested(torrent_entity, peer_entity)
		elif message_id == msg.MessageId.BITFIELD:
			remote_bitfield.update(msg.payload_bitfield(message))
			await self.__update_local_interested(torrent_entity, peer_entity)
		elif message_id == msg.MessageId.INTERESTED:
			self._set_remote_interested(peer_entity, True)
			await self.env.event_bus.dispatch_async("peer.remote.interested_changed", torrent_entity, peer_entity)
		elif message_id == msg.MessageId.NOT_INTERESTED:
			self._set_remote_interested(peer_entity, False)
			await self.env.event_bus.dispatch_async("peer.remote.interested_changed", torrent_entity, peer_entity)

	@staticmethod
	def _set_remote_interested(peer_entity: Entity, value: bool):
		if value and not peer_entity.has_component(RemoteInterestedEC):
			peer_entity.add_component(RemoteInterestedEC())
		elif not value and peer_entity.has_component(RemoteInterestedEC):
			peer_entity.remove_component(RemoteInterestedEC)

	async def __update_local_interested(self, torrent_entity: Entity, peer_entity: Entity):
		if not torrent_entity.has_component(TorrentInfoEC):
			return
		remote_bitfield = peer_entity.get_component(PeerEC).remote_bitfield
		want = len(interested_pieces(torrent_entity, remote_bitfield)) > 0
		if want and not peer_entity.has_component(LocalInterestedEC):
			if self._interested_count() >= self.env.config.download_peers_limit:
				return
		await self._set_local_interested(torrent_entity, peer_entity, want)

	def _interested_count(self) -> int:
		return sum(
			1 for e in self.env.data_storage.get_collection(PeerConnectionEC)
			if e.has_component(LocalInterestedEC)
		)

	async def _set_local_interested(self, torrent_entity: Entity, peer_entity: Entity, want: bool):
		if not peer_entity.has_component(PeerConnectionEC):
			return
		has = peer_entity.has_component(LocalInterestedEC)
		if want and not has:
			peer_entity.add_component(LocalInterestedEC())
			await peer_entity.get_component(PeerConnectionEC).send(msg.interested())
			await self.env.event_bus.dispatch_async("peer.local.interested_changed", torrent_entity, peer_entity)
		elif has and not want:
			peer_entity.remove_component(LocalInterestedEC)
			await peer_entity.get_component(PeerConnectionEC).send(msg.not_interested())
			await self.env.event_bus.dispatch_async("peer.local.interested_changed", torrent_entity, peer_entity)
