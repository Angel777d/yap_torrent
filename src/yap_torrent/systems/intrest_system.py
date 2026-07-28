import logging
from typing import Set

from angelovich.core.DataStorage import Entity

from yap_torrent.components.peer_ec import (
	FullPeerEC,
	LocalInterestedEC,
	PeerConnectionEC,
	PeerEC,
	RemoteInterestedEC,
)
from yap_torrent.components.torrent_ec import TorrentDownloadProgressEC, TorrentEC, TorrentInfoEC
from yap_torrent.protocol import bt_main_messages as msg
from yap_torrent.protocol.message import Message
from yap_torrent.system import System
from yap_torrent.systems import get_info_hash, iterate_peers


def interested_pieces(torrent_entity: Entity, remote_bitfield) -> Set[int]:
	"""Pieces the remote has that we want and lack (wanted-masked)."""
	local = torrent_entity.get_component(TorrentEC).bitfield
	missing = local.interested_in(remote_bitfield)  # remote has, we don't
	if torrent_entity.has_component(TorrentDownloadProgressEC):
		wanted = torrent_entity.get_component(TorrentDownloadProgressEC).wanted
		return {i for i in missing if wanted.have_index(i)}
	return missing


logger = logging.getLogger(__name__)


class InterestedSystem(System):
	_INTERESTED_MESSAGES = (msg.MessageId.INTERESTED.value, msg.MessageId.NOT_INTERESTED.value,
	                        msg.MessageId.HAVE.value, msg.MessageId.BITFIELD.value)

	async def start(self):
		self.add_listener("peer.message", self.__on_message)
		self.add_listener("piece.complete", self.__on_piece_complete)
		self.add_listener("peer.connected", self.__on_peer_connected)
		self.add_listener("peer.disconnected", self._on_peer_disconnected)
		self.add_listener("action.torrent.stop", self._on_torrent_stop)
		self.add_listener("action.torrent.start", self._on_torrent_start)

	async def _on_torrent_stop(self, torrent_entity: Entity):
		for peer_entity in list(iterate_peers(self.env, get_info_hash(torrent_entity))):
			await self._set_local_interested(torrent_entity, peer_entity, False)

	async def _on_torrent_start(self, torrent_entity: Entity):
		for peer_entity in list(iterate_peers(self.env, get_info_hash(torrent_entity))):
			await self.update_local_interested(torrent_entity, peer_entity)

	async def __on_peer_connected(self, torrent_entity: Entity, peer_entity: Entity):
		await self.update_local_interested(torrent_entity, peer_entity)

	async def __on_piece_complete(self, torrent_entity: Entity, piece_entity: Entity):
		from yap_torrent.components.piece_ec import PieceEC
		info_hash = get_info_hash(torrent_entity)
		index = piece_entity.get_component(PieceEC).info.index

		for peer_entity in list(iterate_peers(self.env, info_hash)):
			await peer_entity.get_component(PeerConnectionEC).send(msg.have(index))
			await self.update_local_interested(torrent_entity, peer_entity)

	async def __on_message(self, torrent_entity: Entity, peer_entity: Entity, message: Message):
		if message.message_id not in self._INTERESTED_MESSAGES:
			return

		peer_ec = peer_entity.get_component(PeerEC)
		remote_bitfield = peer_ec.remote_bitfield
		message_id = msg.MessageId(message.message_id)

		if message_id == msg.MessageId.HAVE:
			remote_bitfield.set_index(msg.payload_index(message))
			self._update_full(torrent_entity, peer_entity, remote_bitfield)
			await self.update_local_interested(torrent_entity, peer_entity)
		elif message_id == msg.MessageId.BITFIELD:
			remote_bitfield.update(msg.payload_bitfield(message))
			self._update_full(torrent_entity, peer_entity, remote_bitfield)
			await self.update_local_interested(torrent_entity, peer_entity)
		elif message_id == msg.MessageId.INTERESTED:
			self._set_remote_interested(peer_entity, True)
			await self.env.event_bus.dispatch_async("peer.remote.interested_changed", torrent_entity, peer_entity)
		elif message_id == msg.MessageId.NOT_INTERESTED:
			self._set_remote_interested(peer_entity, False)
			await self.env.event_bus.dispatch_async("peer.remote.interested_changed", torrent_entity, peer_entity)

	def _update_full(self, torrent_entity: Entity, peer_entity: Entity, remote_bitfield):
		if not torrent_entity.has_component(TorrentInfoEC):
			return
		pieces_num = torrent_entity.get_component(TorrentInfoEC).info.pieces_num
		is_full = remote_bitfield.have_num >= pieces_num
		if is_full and not peer_entity.has_component(FullPeerEC):
			peer_entity.add_component(FullPeerEC())
		elif not is_full and peer_entity.has_component(FullPeerEC):
			peer_entity.remove_component(FullPeerEC)

	@staticmethod
	def _set_remote_interested(peer_entity: Entity, value: bool):
		if value and not peer_entity.has_component(RemoteInterestedEC):
			peer_entity.add_component(RemoteInterestedEC())
		elif not value and peer_entity.has_component(RemoteInterestedEC):
			peer_entity.remove_component(RemoteInterestedEC)

	async def update_local_interested(self, torrent_entity: Entity, peer_entity: Entity):
		if not torrent_entity.has_component(TorrentInfoEC):
			return
		remote_bitfield = peer_entity.get_component(PeerEC).remote_bitfield
		want = len(interested_pieces(torrent_entity, remote_bitfield)) > 0
		# LocalInterestedEC is half the download queue, so this counter is what caps it
		if want and not peer_entity.has_component(LocalInterestedEC):
			if self._interested_count(get_info_hash(torrent_entity)) >= self.env.config.download_peers_limit:
				return
		await self._set_local_interested(torrent_entity, peer_entity, want)

	def _interested_count(self, info_hash: bytes) -> int:
		return sum(
			1 for p in iterate_peers(self.env, info_hash) if p.has_component(LocalInterestedEC)
		)

	async def _on_peer_disconnected(self, torrent_entity: Entity, peer_entity: Entity):
		"""Offer the freed download slot to the peers we still hold.

		Otherwise it is only refilled when some other peer happens to send a HAVE/BITFIELD.
		"""
		for other in list(iterate_peers(self.env, get_info_hash(torrent_entity))):
			if other is peer_entity or other.has_component(LocalInterestedEC):
				continue
			await self.update_local_interested(torrent_entity, other)

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
