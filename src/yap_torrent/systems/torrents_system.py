import logging
from typing import Iterable, List, Optional

from angelovich.core.DataStorage import Entity

from yap_torrent.components.torrent_ec import (
	SaveTorrentEC,
	TorrentInfoEC,
	TorrentLabelsEC,
	TorrentLimitsEC,
	TorrentPriorityEC,
	TorrentState,
	TorrentStatsEC,
)
from yap_torrent.system import System
from yap_torrent.systems import get_torrent_entity, get_torrent_name

logger = logging.getLogger(__name__)


def _move_target(direction, index: int, count: int) -> Optional[int]:
	"""Where a move puts a torrent, or None if it is not something we understand.

	Accepts the four directions and an absolute position, because `torrent-set` sets a
	queue position by number while the queue-move methods nudge by direction.
	Clamped rather than wrapped: "up" at the top stays at the top, which is what every
	client expects from a disabled-looking button.
	"""
	if isinstance(direction, bool):
		return None  # bool is an int subclass; a JSON true is not position 1
	if isinstance(direction, int):
		return max(0, min(count - 1, direction))

	targets = {"top": 0, "bottom": count - 1, "up": index - 1, "down": index + 1}
	if direction not in targets:
		return None
	return max(0, min(count - 1, targets[direction]))


def _renumber(ordered: List[Entity]) -> None:
	"""Rewrite the queue as a dense 0..n-1, saving only what actually moved."""
	for position, entity in enumerate(ordered):
		priority_ec = entity.get_component(TorrentPriorityEC)
		if priority_ec.priority != position:
			priority_ec.priority = position
			_mark_for_save(entity)


def _clean_labels(labels: Iterable[str]) -> List[str]:
	"""Strip, drop blanks, and de-duplicate while keeping the order given."""
	seen: List[str] = []
	for label in labels or ():
		text = str(label).strip()
		if text and text not in seen:
			seen.append(text)
	return seen


def _mark_for_save(torrent_entity: Entity) -> None:
	"""Ask LocalDataSystem to write this torrent out on its next tick.

	Pausing and queue order are deliberate choices by the user, but nothing else in this
	path touches the disk — without the marker they only reached it if the client was shut
	down cleanly, and a crash silently undid them.
	"""
	if not torrent_entity.has_component(SaveTorrentEC):
		torrent_entity.add_component(SaveTorrentEC())


class TorrentSystem(System):

	def __init__(self, env):
		super().__init__(env)
		self._priority_counter = 0

	async def start(self):
		self.add_listener("request.torrent.start", self._on_torrent_start)
		self.add_listener("request.torrent.stop", self._on_torrent_stop)
		self.add_listener("request.torrent.remove", self._on_torrent_remove)
		self.add_listener("request.torrent.set_labels", self._on_set_labels)
		self.add_listener("request.torrent.queue_move", self._on_queue_move)
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
		if not entity.has_component(TorrentPriorityEC):
			initial_priority = len(self.env.data_storage.get_collection(TorrentPriorityEC))
			entity.add_component(TorrentPriorityEC(initial_priority))

	async def _on_torrent_start(self, info_hash: bytes):
		torrent_entity = get_torrent_entity(self.env, info_hash)
		if not torrent_entity:
			logger.warning(f"[TorrentSystem] _on_torrent_start: torrent {info_hash.hex()} not found")
			return
		logger.info(f"Start torrent {get_torrent_name(torrent_entity)}")
		torrent_entity.get_component(TorrentStatsEC).state = TorrentState.Active
		_mark_for_save(torrent_entity)
		await self.env.event_bus.dispatch_async("action.torrent.start", torrent_entity)

	async def _on_torrent_stop(self, info_hash: bytes):
		torrent_entity = get_torrent_entity(self.env, info_hash)
		if not torrent_entity:
			logger.warning(f"[TorrentSystem] _on_torrent_stop: torrent {info_hash.hex()} not found")
			return
		logger.info(f"Stop torrent {get_torrent_name(torrent_entity)}")
		torrent_entity.get_component(TorrentStatsEC).state = TorrentState.Inactive
		_mark_for_save(torrent_entity)
		await self.env.event_bus.dispatch_async("action.torrent.stop", torrent_entity)

	async def _on_set_labels(self, info_hash: bytes, labels: Iterable[str]):
		torrent_entity = get_torrent_entity(self.env, info_hash)
		if not torrent_entity:
			logger.warning(f"[TorrentSystem] _on_set_labels: torrent {info_hash.hex()} not found")
			return

		cleaned = _clean_labels(labels)
		if torrent_entity.has_component(TorrentLabelsEC):
			if cleaned == torrent_entity.get_component(TorrentLabelsEC).labels:
				return
			torrent_entity.get_component(TorrentLabelsEC).labels = cleaned
		elif cleaned:
			torrent_entity.add_component(TorrentLabelsEC(cleaned))
		else:
			return
		_mark_for_save(torrent_entity)

	async def _on_set_limits(self, info_hash: bytes, values: dict):
		"""Store per-torrent bandwidth/seeding preferences. Nothing enforces them.

		TODO: wire these into the transfer path. Until then the warning below is the
		 only thing telling an operator that a limit they set is doing nothing.
		"""
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
		logger.warning("Stored per-torrent limits %s for %s, but NOTHING ENFORCES THEM: "
		               "there is no bandwidth limiting or ratio-based stopping in core",
		               ", ".join(sorted(changed)), get_torrent_name(torrent_entity))
		_mark_for_save(torrent_entity)

	async def _on_queue_move(self, info_hash: bytes, direction):
		"""Move a torrent within the queue: top / up / down / bottom, or a position number.

		The queue is a dense 0..n-1 ordering, so a move is a reinsertion followed by a
		renumber — not an arithmetic nudge of one priority, which would collide with a
		neighbour and leave two torrents claiming the same place.
		"""
		torrent_entity = get_torrent_entity(self.env, info_hash)
		if not torrent_entity or not torrent_entity.has_component(TorrentPriorityEC):
			logger.warning(f"[TorrentSystem] _on_queue_move: torrent {info_hash.hex()} is not in the queue")
			return

		ordered = self._ordered_by_priority()
		index = ordered.index(torrent_entity)
		target = _move_target(direction, index, len(ordered))
		if target is None:
			logger.warning(f"[TorrentSystem] _on_queue_move: unknown direction '{direction}'")
			return
		if target == index:
			return

		ordered.insert(target, ordered.pop(index))
		_renumber(ordered)

	def _ordered_by_priority(self) -> List[Entity]:
		return sorted(self.env.data_storage.get_collection(TorrentPriorityEC),
		              key=lambda e: e.get_component(TorrentPriorityEC).priority)

	async def _on_torrent_remove(self, info_hash: bytes, _delete_data: bool = False):
		torrent_entity = get_torrent_entity(self.env, info_hash)
		if not torrent_entity:
			logger.warning(f"[TorrentSystem] can't remove torrent {info_hash.hex()}. Not found")
			return
		logger.info(f"Remove torrent {get_torrent_name(torrent_entity)}")
		await self._on_torrent_stop(info_hash)
		await self.env.event_bus.dispatch_async("action.torrent.remove", info_hash)
		self.env.data_storage.remove_entity(get_torrent_entity(self.env, info_hash))

		# close the gap the removed torrent left
		_renumber(self._ordered_by_priority())
