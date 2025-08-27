import logging
import time

from angelovichcore.DataStorage import Entity
from torrent_app import System
from torrent_app.components.bitfield_ec import BitfieldEC
from torrent_app.components.peer_ec import PeerInfoEC, PeerPendingEC, KnownPeersEC, KnownPeersUpdateEC
from torrent_app.components.torrent_ec import TorrentInfoEC, TorrentHashEC
from torrent_app.components.tracker_ec import TorrentTrackerDataEC
from torrent_app.protocol.tracker import make_announce

logger = logging.getLogger(__name__)


class AnnounceSystem(System):

	async def _update(self, delta_time: float):
		event = "started"  # "started", "completed", "stopped"
		current_time = time.monotonic()
		ds = self.env.data_storage

		# process updated trackers
		updates_collection = ds.get_collection(KnownPeersUpdateEC).entities
		for torrent_entity in updates_collection:
			torrent_entity.remove_component(KnownPeersUpdateEC)
			info_hash: bytes = torrent_entity.get_component(TorrentHashEC).info_hash
			peers = torrent_entity.get_component(KnownPeersEC).peers
			for peer in peers:
				if not ds.get_collection(PeerInfoEC).find(PeerInfoEC.make_hash(info_hash, peer)):
					ds.create_entity().add_component(PeerInfoEC(info_hash, peer)).add_component(
						PeerPendingEC())

		# make announces
		trackers_collection = ds.get_collection(TorrentTrackerDataEC).entities
		for torrent_entity in trackers_collection:
			tracker_ec = torrent_entity.get_component(TorrentTrackerDataEC)
			interval = min(tracker_ec.interval, tracker_ec.min_interval)
			if tracker_ec.last_update_time + interval <= current_time:
				await self.__tracker_announce(torrent_entity, event)

	async def __tracker_announce(self, torrent_entity: Entity, event: str = "started"):
		peer_id = self.env.peer_id
		external_ip = self.env.external_ip
		port = self.env.config.port
		info_hash = torrent_entity.get_component(TorrentHashEC).info_hash
		tracker_ec = torrent_entity.get_component(TorrentTrackerDataEC)
		peers_ec = torrent_entity.get_component(KnownPeersEC)
		bitfield_ec = torrent_entity.get_component(BitfieldEC)
		torrent_info = torrent_entity.get_component(TorrentInfoEC).info

		downloaded = bitfield_ec.have_num * torrent_info.pieces.piece_length
		left = max(torrent_info.size - downloaded, 0)

		uploaded = torrent_entity.get_component(TorrentTrackerDataEC).uploaded

		# TODO: support announce-list format
		# https://bittorrent.org/beps/bep_0012.html
		for announce_group in tracker_ec.announce_list:
			for announce in announce_group:
				logger.info(f"make announce to: {announce}")

				result = make_announce(
					announce,
					info_hash,
					peer_id=peer_id,
					downloaded=downloaded,
					uploaded=uploaded,
					left=left,
					port=port,
					ip=external_ip,
					event=event,
					compact=1,
					tracker_id=tracker_ec.tracker_id
				)

				if result and not result.failure_reason:
					tracker_ec.save_announce(result)
					peers_ec.update_peers(result.peers)
					tracker_ec.add_marker(KnownPeersUpdateEC)
					return

		# TODO: make it better
		logger.warning("WTF: no announce results")

		tracker_ec.last_update_time = time.monotonic()
		tracker_ec.min_interval = tracker_ec.interval = 60 * 5  # retry in 5 min
