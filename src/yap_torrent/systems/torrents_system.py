import logging
from typing import Iterator

from angelovich.core.DataStorage import Entity, DataStorage

from yap_torrent.components.torrent_ec import (
	ActiveTorrentEC,
	TorrentInfoEC,
	TorrentPriorityEC,
	TorrentState,
	TorrentStatsEC,
	ValidateTorrentEC,
)
from yap_torrent.system import System
from yap_torrent.systems import get_torrent_entity, get_torrent_name, is_torrent_complete

logger = logging.getLogger(__name__)


def iterate_torrents_to_download(ds: DataStorage) -> Iterator[Entity]:
	# use only torrents with TorrentInfoEC
	for torrent_entity in ds.get_collection(TorrentPriorityEC):
		# skip validation torrents
		if torrent_entity.has_component(ValidateTorrentEC):
			continue
		# skip non-active by user torrents
		if torrent_entity.get_component(TorrentStatsEC).state != TorrentState.Active:
			continue
		# skip completed torrents
		if is_torrent_complete(torrent_entity):
			continue
		yield torrent_entity


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

	async def __on_torrent_added(self, entity: Entity, component: TorrentInfoEC):
		# a restored torrent already carries its saved queue position; a new one goes last
		if not entity.has_component(TorrentPriorityEC):
			initial_priority = len(self.env.data_storage.get_collection(TorrentPriorityEC))
			entity.add_component(TorrentPriorityEC(initial_priority))

		await self._update_active_download_queue()

	async def _on_queue_changed(self, _torrent_entity: Entity):
		await self._update_active_download_queue()

	async def _update_active_download_queue(self):
		new_queue = set(sorted(
			iterate_torrents_to_download(self.env.data_storage),
			key=lambda e: e.get_component(TorrentPriorityEC).priority
		)[:self.env.config.max_active_downloads])
		old_queue = set(self.env.data_storage.get_collection(ActiveTorrentEC))

		for entity in new_queue.difference(old_queue):
			entity.add_component(ActiveTorrentEC())
		for entity in old_queue.difference(new_queue):
			entity.remove_component(ActiveTorrentEC)

	async def _on_torrent_start(self, info_hash: bytes):
		torrent_entity = get_torrent_entity(self.env, info_hash)
		if not torrent_entity:
			logger.warning(f"[TorrentSystem] _on_torrent_start: torrent {info_hash.hex()} not found")
			return
		logger.info(f"Start torrent {get_torrent_name(torrent_entity)}")
		torrent_entity.get_component(TorrentStatsEC).state = TorrentState.Active
		await self.env.event_bus.dispatch_async("action.torrent.start", torrent_entity)

	async def _on_torrent_stop(self, info_hash: bytes):
		torrent_entity = get_torrent_entity(self.env, info_hash)
		if not torrent_entity:
			logger.warning(f"[TorrentSystem] _on_torrent_stop: torrent {info_hash.hex()} not found")
			return
		logger.info(f"Stop torrent {get_torrent_name(torrent_entity)}")
		torrent_entity.get_component(TorrentStatsEC).state = TorrentState.Inactive
		await self.env.event_bus.dispatch_async("action.torrent.stop", torrent_entity)

	async def _on_torrent_remove(self, info_hash: bytes, delete_data: bool = False):
		torrent_entity = get_torrent_entity(self.env, info_hash)
		if not torrent_entity:
			logger.warning(f"[TorrentSystem] can't remove torrent {info_hash.hex()}. Not found")
			return
		logger.info(f"Remove torrent {get_torrent_name(torrent_entity)}")
		await self._on_torrent_stop(info_hash)
		await self.env.event_bus.dispatch_async("action.torrent.remove", info_hash)
		self.env.data_storage.remove_entity(get_torrent_entity(self.env, info_hash))

		# restore priorities after remove
		for index, entity in enumerate(sorted(
				self.env.data_storage.get_collection(TorrentPriorityEC),
				key=lambda e: e.get_component(TorrentPriorityEC).priority)):
			entity.get_component(TorrentPriorityEC).priority = index
