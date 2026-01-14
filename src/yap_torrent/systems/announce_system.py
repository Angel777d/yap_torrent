import logging
import time

from angelovich.core.DataStorage import Entity

from yap_torrent.components.torrent_ec import TorrentInfoEC, TorrentEC, TorrentStatsEC
from yap_torrent.components.tracker_ec import TorrentTrackerDataEC, TorrentTrackerEC
from yap_torrent.env import Env
from yap_torrent.protocol.tracker import make_announce
from yap_torrent.system import System
from yap_torrent.systems import get_torrent_name, is_torrent_complete, get_torrent_entity, is_torrent_active

logger = logging.getLogger(__name__)


def _iterate_active_torrents(env: Env):
	trackers_collection = env.data_storage.get_collection(TorrentTrackerDataEC).entities
	for torrent_entity in trackers_collection:
		# skip completed torrents
		if is_torrent_complete(torrent_entity):
			continue
		# skip inactive torrents
		if not is_torrent_active(torrent_entity):
			continue

		tracker_data_ec = torrent_entity.get_component(TorrentTrackerDataEC)

		# skip failed trackers
		if tracker_data_ec.failure_reason:
			continue

		yield torrent_entity


class AnnounceSystem(System):

	async def start(self):
		await super().start()
		self.env.event_bus.add_listener("action.torrent.complete", self._on_torrent_complete, scope=self)
		self.env.event_bus.add_listener("action.torrent.stop", self._on_torrent_stop, scope=self)
		self.env.event_bus.add_listener("action.torrent.start", self._on_torrent_start, scope=self)

	def close(self) -> None:
		self.env.event_bus.remove_all_listeners(scope=self)

		# make "stopped" announcements
		for torrent_entity in _iterate_active_torrents(self.env):
			tracker_data_ec = torrent_entity.get_component(TorrentTrackerDataEC)
			if not tracker_data_ec.started:
				return
			self.__tracker_announce(torrent_entity, "stopped")

	async def _on_torrent_complete(self, torrent_entity: Entity):
		await self.__tracker_announce_async(torrent_entity, "completed")

	async def _on_torrent_start(self, info_hash: bytes):
		torrent_entity = get_torrent_entity(self.env, info_hash)
		torrent_entity.get_component(TorrentTrackerDataEC).started = True
		await self.__tracker_announce_async(torrent_entity, "started")

	async def _on_torrent_stop(self, info_hash: bytes):
		torrent_entity = get_torrent_entity(self.env, info_hash)
		torrent_entity.get_component(TorrentTrackerDataEC).started = False
		await self.__tracker_announce_async(torrent_entity, "stopped")

	async def _update(self, delta_time: float):
		current_time = time.monotonic()
		ds = self.env.data_storage

		# make announcement
		for torrent_entity in _iterate_active_torrents(self.env):
			tracker_data_ec = torrent_entity.get_component(TorrentTrackerDataEC)

			# TODO: make start event on startup and
			event = ""  # empty, "started", "completed", "stopped"
			if not tracker_data_ec.started:
				tracker_data_ec.started = True
				event = "started"

			interval = min(tracker_data_ec.interval, tracker_data_ec.min_interval)
			if tracker_data_ec.last_update_time + interval <= current_time:
				await self.__tracker_announce_async(torrent_entity, event)

	async def __tracker_announce_async(self, torrent_entity: Entity, event: str = "started"):
		self.__tracker_announce(torrent_entity, event)

	def __tracker_announce(self, torrent_entity: Entity, event: str = "started"):
		peer_id = self.env.peer_id
		external_ip = self.env.external_ip
		port = self.env.config.port
		info_hash = torrent_entity.get_component(TorrentEC).info_hash
		bitfield = torrent_entity.get_component(TorrentEC).bitfield
		tracker_ec = torrent_entity.get_component(TorrentTrackerEC)
		tracker_data_ec = torrent_entity.get_component(TorrentTrackerDataEC)
		torrent_name = get_torrent_name(torrent_entity)

		left = 0
		if torrent_entity.has_component(TorrentInfoEC):
			torrent_info = torrent_entity.get_component(TorrentInfoEC).info
			left = max(torrent_info.size - bitfield.have_num * torrent_info.piece_length, 0)

		uploaded = torrent_entity.get_component(TorrentStatsEC).uploaded
		downloaded = torrent_entity.get_component(TorrentStatsEC).downloaded

		# https://bittorrent.org/beps/bep_0012.html
		for announce_tier in tracker_ec.announce_list:
			for announce in announce_tier:
				logger.info(f"make announce '{event}' to: {announce}")
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
					tracker_id=tracker_data_ec.tracker_id
				)

				if not result:
					logger.info(f"announce to {announce} failed")
					continue

				# update tracker data
				tracker_data_ec.save_announce(result)

				# move good announce to the front of the tier for next time
				announce_tier.remove(announce)
				announce_tier.insert(0, announce)

				# stop future updates in case of error
				if result.failure_reason:
					logger.warning(
						f"Torrent tracker '{announce}' says '{result.failure_reason}'. Torrent {torrent_name} looks broken.")
					return

				# log warning
				if result.warning_message:
					logger.warning(f"Announce warning '{result.warning_message}' for {torrent_name}")

				# update peers got from tracker
				logger.info(f"Announce to '{announce}' for {torrent_name} succeeded. Got {len(result.peers)} peers")
				self.env.event_bus.dispatch("peers.update", info_hash, result.peers)
				return

		# we couldn't get any data from trackers.
		logger.warning(f"No announce results for {torrent_name}")
		tracker_data_ec.fail_announce()
