import logging
from typing import Iterable, Set, Tuple, TypeVar

from angelovich.core.DataStorage import Entity

from yap_torrent.components.torrent_ec import (
	ActiveTorrentEC,
	InProgressEC,
	TorrentEC,
	TorrentInfoEC,
	TorrentPriorityEC,
	TorrentState,
	TorrentStatsEC,
	ValidateTorrentEC,
)
from yap_torrent.system import System
from yap_torrent.systems import compute_wanted_bitfield, get_torrent_entity, is_torrent_complete

logger = logging.getLogger(__name__)

_K = TypeVar("_K")


def select_active(items: Iterable[Tuple[_K, int]], limit: int) -> Set[_K]:
	"""Return the keys of the ``limit`` lowest-priority items (lower = served first).

	Pure helper for the download-queue active window. ``items`` is (key, priority);
	ties break on the key so the result is deterministic.
	"""
	if limit <= 0:
		return set()
	ordered = sorted(items, key=lambda kv: (kv[1], kv[0]))
	return {key for key, _ in ordered[:limit]}


class TorrentSystem(System):

	def __init__(self, env):
		super().__init__(env)
		self._priority_counter = 0

	async def start(self):
		self.add_listener("request.torrent.start", self._on_torrent_start)
		self.add_listener("request.torrent.stop", self._on_torrent_stop)
		self.add_listener("request.torrent.remove", self._on_torrent_remove)
		# recompute the active-download window on any state transition
		self.add_listener("action.torrent.start", self._on_queue_changed)
		self.add_listener("action.torrent.stop", self._on_queue_changed)
		self.add_listener("action.torrent.complete", self._on_queue_changed)

	async def _update(self, delta_time: float):
		# light periodic recompute — also covers metadata arriving / validation ending
		self._recompute_queue()

	async def _on_queue_changed(self, _torrent_entity: Entity):
		self._recompute_queue()

	async def _on_torrent_start(self, info_hash: bytes):
		torrent_entity = get_torrent_entity(self.env, info_hash)
		if not torrent_entity:
			logger.warning(f"[TorrentSystem] _on_torrent_start: torrent {info_hash.hex()} not found")
			return
		torrent_entity.get_component(TorrentStatsEC).state = TorrentState.Active
		await self.env.event_bus.dispatch_async("action.torrent.start", torrent_entity)

	async def _on_torrent_stop(self, info_hash: bytes):
		logger.info(f"Stopping torrent {info_hash.hex()}")
		torrent_entity = get_torrent_entity(self.env, info_hash)
		if not torrent_entity:
			logger.warning(f"[TorrentSystem] _on_torrent_stop: torrent {info_hash.hex()} not found")
			return
		torrent_entity.get_component(TorrentStatsEC).state = TorrentState.Inactive
		await self.env.event_bus.dispatch_async("action.torrent.stop", torrent_entity)
		logger.info(f"Stopping torrent {info_hash.hex()} complete")

	async def _on_torrent_remove(self, info_hash: bytes, delete_data: bool = False):
		logger.info(f"Remove torrent {info_hash.hex()}")
		torrent_entity = get_torrent_entity(self.env, info_hash)
		if not torrent_entity:
			logger.warning(f"[TorrentSystem] can't remove torrent {info_hash.hex()}. Not found")
			return
		await self._on_torrent_stop(info_hash)
		await self.env.event_bus.dispatch_async("action.torrent.remove", info_hash)
		self.env.data_storage.remove_entity(get_torrent_entity(self.env, info_hash))
		logger.info(f"Remove torrent {info_hash.hex()} complete")

	# -- download queue ----------------------------------------------------
	def _is_eligible(self, torrent_entity: Entity) -> bool:
		"""Active, has metadata, not verifying, not complete."""
		if torrent_entity.get_component(TorrentStatsEC).state != TorrentState.Active:
			return False
		if not torrent_entity.has_component(TorrentInfoEC):
			return False
		if torrent_entity.has_component(ValidateTorrentEC):
			return False
		return not is_torrent_complete(torrent_entity)

	def _recompute_queue(self):
		eligible = []
		for torrent_entity in self.env.data_storage.get_collection(TorrentEC):
			# the wanted-piece set is present whenever metadata is known — it drives
			# interest + selection AND wanted-aware completion, so it must NOT be removed
			# when the torrent leaves the queue (that would flip is_torrent_complete).
			if torrent_entity.has_component(TorrentInfoEC) and not torrent_entity.has_component(InProgressEC):
				in_progress = InProgressEC()
				in_progress.wanted = compute_wanted_bitfield(self.env, torrent_entity)
				torrent_entity.add_component(in_progress)

			if self._is_eligible(torrent_entity):
				# organic priority: assign in the order torrents become eligible
				if not torrent_entity.has_component(TorrentPriorityEC):
					self._priority_counter += 1
					torrent_entity.add_component(TorrentPriorityEC(self._priority_counter))
				eligible.append(torrent_entity)
			else:
				# ineligible (inactive / complete / verifying / no metadata): leave the queue
				for component in (ActiveTorrentEC, TorrentPriorityEC):
					if torrent_entity.has_component(component):
						torrent_entity.remove_component(component)

		limit = self.env.config.max_active_downloads
		by_hash = {e.get_component(TorrentEC).info_hash: e for e in eligible}
		active = select_active(
			((info_hash, e.get_component(TorrentPriorityEC).priority) for info_hash, e in by_hash.items()),
			limit,
		)

		for info_hash, torrent_entity in by_hash.items():
			want_active = info_hash in active
			has_active = torrent_entity.has_component(ActiveTorrentEC)
			if want_active and not has_active:
				torrent_entity.add_component(ActiveTorrentEC())
			elif has_active and not want_active:
				torrent_entity.remove_component(ActiveTorrentEC)
