import logging
from typing import Iterable, List

from angelovich.core.DataStorage import Entity
from angelovich.core.System import System

from yap_torrent.components.torrent_ec import (
	TorrentInfoEC,
	TorrentLimitsEC,
	TorrentQueuePositionEC,
	TorrentState,
	TorrentStatsEC,
)
from yap_torrent.protocol import InfoHash
from yap_torrent.systems import get_info_hash, get_torrent_entity, get_torrent_name, mark_for_save

logger = logging.getLogger(__name__)


def _renumber(ordered: List[Entity]) -> None:
	"""Rewrite the queue as a dense 0..n-1, saving only what actually moved."""
	for position, entity in enumerate(ordered):
		position_ec = entity.get_component(TorrentQueuePositionEC)
		if position_ec.position != position:
			position_ec.position = position
			mark_for_save(entity)


class TorrentSystem(System):

	def __init__(self, env):
		super().__init__(env)
		self._priority_counter = 0

	async def start(self):
		self.add_listener("request.torrent.start", self._on_torrent_start)
		self.add_listener("request.torrent.stop", self._on_torrent_stop)
		self.add_listener("request.torrent.remove", self._on_torrent_remove)
		self.add_listener("request.torrent.queue_order", self._on_queue_order)
		self.add_listener("request.torrent.set_limits", self._on_set_limits)

		# subscribe to new torrents
		collection = self.env.data_storage.get_collection(TorrentInfoEC)
		collection.add_listener(collection.EVENT_ADDED, self.__on_torrent_added, self)

		# iterate on existing torrents
		for entity in collection:
			await self.__on_torrent_added(entity, entity.get_component(TorrentInfoEC))

	async def stop(self):
		collection = self.env.data_storage.get_collection(TorrentInfoEC)
		collection.remove_all_listeners(self)

		return await super().stop()

	async def __on_torrent_added(self, entity: Entity, _component: TorrentInfoEC):
		# a restored torrent already carries its saved queue position; a new one goes last
		if not entity.has_component(TorrentQueuePositionEC):
			last = len(self.env.data_storage.get_collection(TorrentQueuePositionEC))
			entity.add_component(TorrentQueuePositionEC(last))

	async def _on_torrent_start(self, info_hash: bytes):
		torrent_entity = get_torrent_entity(self.env, info_hash)
		if not torrent_entity:
			logger.warning(f"[TorrentSystem] _on_torrent_start: torrent {info_hash.hex()} not found")
			return
		logger.info(f"Start torrent {get_torrent_name(torrent_entity)}")
		torrent_entity.get_component(TorrentStatsEC).state = TorrentState.Active
		mark_for_save(torrent_entity)
		await self.env.event_bus.dispatch_async("action.torrent.start", torrent_entity)

	async def _on_torrent_stop(self, info_hash: bytes):
		torrent_entity = get_torrent_entity(self.env, info_hash)
		if not torrent_entity:
			logger.warning(f"[TorrentSystem] _on_torrent_stop: torrent {info_hash.hex()} not found")
			return
		logger.info(f"Stop torrent {get_torrent_name(torrent_entity)}")
		torrent_entity.get_component(TorrentStatsEC).state = TorrentState.Inactive
		mark_for_save(torrent_entity)
		await self.env.event_bus.dispatch_async("action.torrent.stop", torrent_entity)

	async def _on_set_limits(self, info_hash: bytes, values: dict):
		"""Store per-torrent bandwidth/seeding preferences. TODO: nothing enforces them yet."""
		torrent_entity = get_torrent_entity(self.env, info_hash)
		if not torrent_entity or not values:
			return

		accepted = {key: value for key, value in values.items() if key in TorrentLimitsEC.FIELDS}
		if not accepted:
			return

		if not torrent_entity.has_component(TorrentLimitsEC):
			torrent_entity.add_component(TorrentLimitsEC())
		limits = torrent_entity.get_component(TorrentLimitsEC)

		changed = []
		for key, value in accepted.items():
			cast = type(getattr(limits, key))
			try:
				cast_value = cast(value)
			except (TypeError, ValueError):
				logger.warning("[TorrentSystem] bad value for limit '%s': %r", key, value)
				continue
			if getattr(limits, key) != cast_value:
				setattr(limits, key, cast_value)
				changed.append(key)

		if not changed:
			return
		logger.warning("Stored per-torrent limits %s for %s, but NOTHING ENFORCES THEM YET: "
		               "there is no bandwidth limiting or ratio-based stopping in core",
		               ", ".join(sorted(changed)), get_torrent_name(torrent_entity))
		mark_for_save(torrent_entity)

	async def _on_queue_order(self, ordered_info_hashes: Iterable[InfoHash]):
		"""Set the queue to the order given: entry i takes position i.

		The list *is* the order — there are no directions here. What "up" or "bottom" or a
		position out of range means is the caller's to work out, because it depends on the
		interface the request came from; all the queue needs to hold is the ordinal.

		A torrent in the queue but absent from the list keeps its place relative to the
		others after it, so a caller that only knows about some of them cannot silently
		drop the rest. Unknown hashes and repeats are skipped.
		"""
		in_queue = self._ordered_by_priority()
		by_hash = {get_info_hash(entity): entity for entity in in_queue}

		ordered: List[Entity] = []
		placed = set()
		for info_hash in ordered_info_hashes or ():
			entity = by_hash.get(info_hash)
			if entity is None or info_hash in placed:
				logger.warning("[TorrentSystem] _on_queue_order: skipping %s", info_hash.hex())
				continue
			placed.add(info_hash)
			ordered.append(entity)

		ordered.extend(entity for entity in in_queue if get_info_hash(entity) not in placed)
		_renumber(ordered)

	def _ordered_by_priority(self) -> List[Entity]:
		return sorted(self.env.data_storage.get_collection(TorrentQueuePositionEC),
		              key=lambda e: e.get_component(TorrentQueuePositionEC).position)

	async def _on_torrent_remove(self, info_hash: bytes, _delete_data: bool = False):
		torrent_entity = get_torrent_entity(self.env, info_hash)
		if not torrent_entity:
			logger.warning(f"[TorrentSystem] can't remove torrent {info_hash.hex()}. Not found")
			return
		logger.info(f"Remove torrent {get_torrent_name(torrent_entity)}")
		await self._on_torrent_stop(info_hash)
		await self.env.event_bus.dispatch_async("action.torrent.remove", info_hash)
		self.env.data_storage.remove_entity(torrent_entity)

		# close the gap the removed torrent left
		_renumber(self._ordered_by_priority())
