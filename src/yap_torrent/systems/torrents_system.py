import logging

from angelovich.core.DataStorage import Entity

from yap_torrent.components.torrent_ec import (
	SaveTorrentEC,
	TorrentInfoEC,
	TorrentPriorityEC,
	TorrentState,
	TorrentStatsEC,
)
from yap_torrent.system import System
from yap_torrent.systems import get_torrent_entity, get_torrent_name

logger = logging.getLogger(__name__)


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

	async def _on_torrent_remove(self, info_hash: bytes, _delete_data: bool = False):
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
			if entity.get_component(TorrentPriorityEC).priority != index:
				entity.get_component(TorrentPriorityEC).priority = index
				_mark_for_save(entity)
