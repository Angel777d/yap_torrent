import logging
import time

from angelovichcore.DataStorage import Entity
from torrent_app import System
from torrent_app.components.bitfield_ec import BitfieldEC
from torrent_app.components.peer_ec import PeerInfoEC, PeerPendingEC
from torrent_app.components.torrent_ec import TorrentInfoEC
from torrent_app.components.tracker_ec import TorrentTrackerDataEC, TorrentTrackerUpdatedEC
from torrent_app.protocol import TorrentInfo
from torrent_app.protocol.tracker import make_announce

logger = logging.getLogger(__name__)


class AnnounceSystem(System):

	async def _update(self, delta_time: float):
		event = "started"  # "started", "completed", "stopped"
		current_time = time.monotonic()
		ds = self.env.data_storage

		# process updated trackers
		updates_collection = ds.get_collection(TorrentTrackerUpdatedEC).entities
		for entity in updates_collection:
			entity.remove_component(TorrentTrackerUpdatedEC)
			torrent_info: TorrentInfo = entity.get_component(TorrentInfoEC).info
			peers = entity.get_component(TorrentTrackerDataEC).peers
			for peer in peers:
				if not ds.get_collection(PeerInfoEC).find(PeerInfoEC.make_hash(torrent_info.info_hash, peer)):
					ds.create_entity().add_component(PeerInfoEC(torrent_info.info_hash, peer)).add_component(
						PeerPendingEC())

		# make announces
		trackers_collection = ds.get_collection(TorrentTrackerDataEC).entities
		for entity in trackers_collection:
			tracker_ec = entity.get_component(TorrentTrackerDataEC)
			interval = min(tracker_ec.interval, tracker_ec.min_interval)
			if tracker_ec.last_update_time + interval <= current_time:
				await self.__tracker_announce(entity, event)

	async def __tracker_announce(self, entity: Entity, event: str = "started"):
		peer_id = self.env.peer_id
		external_ip = self.env.external_ip
		port = self.env.config.port
		tracker_ec = entity.get_component(TorrentTrackerDataEC)
		torrent_info = entity.get_component(TorrentInfoEC).info

		torrent_entity = self.env.data_storage.get_collection(TorrentInfoEC).find(torrent_info.info_hash)
		bitfield_ec: BitfieldEC = torrent_entity.get_component(BitfieldEC)
		info = torrent_entity.get_component(TorrentInfoEC).info

		downloaded = bitfield_ec.have_num * info.pieces.piece_length
		left = max(info.size - downloaded, 0)

		uploaded = torrent_entity.get_component(TorrentTrackerDataEC).uploaded

		# TODO: support announce-list format
		# https://bittorrent.org/beps/bep_0012.html
		for announce_group in torrent_info.announce_list:
			for announce in announce_group:
				logger.info(f"make announce to: {announce}")

				result = make_announce(
					announce,
					torrent_info.info_hash,
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
					tracker_ec.add_marker(TorrentTrackerUpdatedEC)
					return

		# TODO: make it better
		logger.warning("WTF: no announce results")

		tracker_ec.last_update_time = time.monotonic()
		tracker_ec.min_interval = tracker_ec.interval = 60 * 5  # retry in 5 min
