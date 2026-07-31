import logging
import time
from typing import Tuple

from angelovich.core.DataStorage import Entity

from yap_torrent.components.peer_ec import PeerEC, PeerRateEC
from yap_torrent.components.torrent_ec import SaveTorrentEC, TorrentEC, TorrentRateEC, TorrentStatsEC
from yap_torrent.env import Env
from yap_torrent.system import TimeSystem
from yap_torrent.systems import get_torrent_entity

logger = logging.getLogger(__name__)

SAMPLE_INTERVAL = 1.0  # seconds between rate samples


def _mark_for_save(torrent_entity: Entity) -> None:
	if not torrent_entity.has_component(SaveTorrentEC):
		torrent_entity.add_component(SaveTorrentEC())


def session_rates(env: Env) -> Tuple[float, float]:
	"""Client-wide (down, up) in bytes/sec — the sum of every torrent's sampled rate."""
	down = up = 0.0
	for torrent_entity in env.data_storage.get_collection(TorrentRateEC):
		rate = torrent_entity.get_component(TorrentRateEC)
		down += rate.down_rate
		up += rate.up_rate
	return down, up


class StatsSystem(TimeSystem):
	"""Samples PeerRateEC into TorrentRateEC. The only caller of sample_rate()."""

	def __init__(self, env: Env):
		super().__init__(env, SAMPLE_INTERVAL)

	async def start(self):
		self.add_listener("action.torrent.start", self._on_torrent_start)
		self.add_listener("action.torrent.complete", self._on_torrent_complete)

	async def _on_torrent_start(self, torrent_entity: Entity):
		stats = torrent_entity.get_component(TorrentStatsEC)
		stats.started_date = time.time()
		_mark_for_save(torrent_entity)

	async def _on_torrent_complete(self, torrent_entity: Entity):
		stats = torrent_entity.get_component(TorrentStatsEC)
		# only the first completion sets the date (re-checks raise the event again)
		if not stats.done_date:
			stats.done_date = time.time()
			_mark_for_save(torrent_entity)

	async def _update(self, delta_time: float):
		now = time.monotonic()
		ds = self.env.data_storage

		# zero first, so a torrent whose peers have gone reads 0 rather than its last value
		for torrent_entity in ds.get_collection(TorrentEC):
			if not torrent_entity.has_component(TorrentRateEC):
				torrent_entity.add_component(TorrentRateEC())
			rate = torrent_entity.get_component(TorrentRateEC)
			rate.down_rate = 0.0
			rate.up_rate = 0.0

		for peer_entity in ds.get_collection(PeerRateEC):
			peer_rate = peer_entity.get_component(PeerRateEC)
			peer_rate.sample_rate(now)

			torrent_entity = get_torrent_entity(self.env, peer_entity.get_component(PeerEC).info_hash)
			if torrent_entity is None:
				continue
			rate = torrent_entity.get_component(TorrentRateEC)
			rate.down_rate += peer_rate.down_rate
			rate.up_rate += peer_rate.up_rate
