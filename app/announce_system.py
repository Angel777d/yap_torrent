import logging
import time

from app import System
from app.components.bitfield_ec import BitfieldEC
from app.components.peer_ec import PeerInfoEC, PeerPendingEC
from app.components.torrent_ec import TorrentInfoEC
from app.components.tracker_ec import TorrentTrackerDataEC, TorrentTrackerUpdatedEC
from torrent.tracker import make_announce

logger = logging.getLogger(__name__)


class AnnounceSystem(System):

	async def _update(self, delta_time: float):
		event = "started"  # "started", "completed", "stopped"
		current_time = time.time()
		ds = self.env.data_storage

		# initiate tracker updates
		trackers_collection = ds.get_collection(TorrentTrackerDataEC).entities
		for entity in trackers_collection:
			tracker_ec = entity.get_component(TorrentTrackerDataEC)
			interval = min(tracker_ec.interval, tracker_ec.min_interval)
			if tracker_ec.last_update_time + interval <= current_time:
				await self.__tracker_announce(tracker_ec, event)

		# create new peers
		updates_collection = ds.get_collection(TorrentTrackerUpdatedEC).entities
		for entity in updates_collection:
			entity.remove_component(TorrentTrackerUpdatedEC)
			tracker_ec = entity.get_component(TorrentTrackerDataEC)
			peers = tracker_ec.peers
			for peer in peers:
				if not ds.get_collection(PeerInfoEC).find(PeerInfoEC.make_hash(tracker_ec.info_hash, peer)):
					ds.create_entity().add_component(PeerInfoEC(tracker_ec.info_hash, peer)).add_component(
						PeerPendingEC())

	async def __tracker_announce(self, tracker_ec: TorrentTrackerDataEC, event: str = "started"):
		peer_id = self.env.peer_id
		external_ip = self.env.external_ip
		port = self.env.config.port

		torrent_entity = self.env.data_storage.get_collection(TorrentInfoEC).find(tracker_ec.info_hash)
		bitfield_ec = torrent_entity.get_component(BitfieldEC)
		info = torrent_entity.get_component(TorrentInfoEC).info

		downloaded = bitfield_ec.have_num * info.pieces.piece_length
		left = max(info.size - downloaded, 0)

		# TODO: track uploaded data size
		uploaded = 0

		# TODO: support announce-list format
		# https://bittorrent.org/beps/bep_0012.html
		for announce_group in tracker_ec.announce_list:
			for announce in announce_group:

				logger.info(f"make announce to: {announce}")

				result = make_announce(announce, tracker_ec.info_hash, peer_id=peer_id, downloaded=downloaded,
				                       uploaded=uploaded, left=left, port=port, ip=external_ip, event=event, compact=1,
				                       tracker_id=tracker_ec.tracker_id)

				if result and not result.failure_reason:
					tracker_ec.save_announce(result)
					tracker_ec.add_marker(TorrentTrackerUpdatedEC)
					return

		# TODO: make it better
		logger.warning("WTF: no announce results")

		tracker_ec.last_update_time = time.time()
		tracker_ec.interval = 60 * 5  # retry in 5 min
