import logging
import time
from typing import Tuple

from yap_torrent.components.peer_ec import PeerEC, PeerRateEC
from yap_torrent.components.torrent_ec import TorrentEC, TorrentRateEC
from yap_torrent.env import Env
from yap_torrent.system import TimeSystem
from yap_torrent.systems import get_torrent_entity

logger = logging.getLogger(__name__)

SAMPLE_INTERVAL = 1.0  # seconds between rate samples


def session_rates(env: Env) -> Tuple[float, float]:
	"""Client-wide (down, up) in bytes/sec — the sum of every torrent's sampled rate."""
	down = up = 0.0
	for torrent_entity in env.data_storage.get_collection(TorrentRateEC):
		rate = torrent_entity.get_component(TorrentRateEC)
		down += rate.down_rate
		up += rate.up_rate
	return down, up


class StatsSystem(TimeSystem):
	"""Turns the byte counters the transfer path fills into sampled rates.

	`PeerRateEC` accumulates a window on every block, but nothing sampled it, so no rate
	ever reached a UI or the RPC. Sampling lives in exactly one place on purpose:
	`sample_rate()` resets the window it reads, so a second caller elsewhere would silently
	halve everyone's numbers.
	"""

	def __init__(self, env: Env):
		super().__init__(env, SAMPLE_INTERVAL)

	async def _update(self, delta_time: float):
		now = time.monotonic()
		ds = self.env.data_storage

		# zero first: a torrent whose peers have all gone keeps whatever it last read
		# otherwise, and reports a transfer that stopped
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
