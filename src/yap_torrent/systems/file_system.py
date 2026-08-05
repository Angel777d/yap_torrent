import logging
from pathlib import Path
from typing import Iterable, Optional

from angelovich.core.DataStorage import Entity
from angelovich.core.System import System

from yap_torrent.components.file_ec import (
	FilePriority,
	RestoreFileSelectionEC,
	TorrentFileEC,
	TorrentFileStateEC,
)
from yap_torrent.components.torrent_ec import SaveTorrentEC, TorrentInfoEC, TorrentDownloadProgressEC
from yap_torrent.protocol import InfoHash
from yap_torrent.systems import get_info_hash, iterate_files, get_torrent_entity, compute_wanted_bitfield

logger = logging.getLogger(__name__)


class FileSystem(System):
	async def start(self):
		await super().start()
		self.add_listener("request.file.select", self._on_file_select)
		self.add_listener("action.torrent.remove", self._on_torrent_remove)

		collection = self.env.data_storage.get_collection(TorrentInfoEC)
		collection.add_listener(collection.EVENT_ADDED, self._on_info_added, self)

		for torrent_entity in collection:
			self._create_file_entities(torrent_entity)

	async def stop(self):
		collection = self.env.data_storage.get_collection(TorrentInfoEC)
		collection.remove_all_listeners(self)
		await super().stop()

	async def _on_file_select(self, info_hash: InfoHash, indices: Optional[Iterable[int]] = None,
	                          wanted: Optional[bool] = None, priority: Optional[int] = None):
		"""Change what a torrent downloads. `indices` of None means every file."""
		torrent_entity = get_torrent_entity(self.env, info_hash)
		if not torrent_entity or not torrent_entity.has_component(TorrentInfoEC):
			return
		if wanted is None and priority is None:
			return

		selected = None if indices is None else set(indices)
		changed = False
		for file_entity in iterate_files(self.env, info_hash):
			if selected is not None and file_entity.get_component(TorrentFileEC).index not in selected:
				continue
			state = file_entity.get_component(TorrentFileStateEC)
			if wanted is not None and state.wanted != bool(wanted):
				state.wanted = bool(wanted)
				changed = True
			if priority is not None and state.priority != FilePriority(priority):
				state.priority = FilePriority(priority)
				changed = True

		if not changed:
			return

		if torrent_entity.has_component(TorrentDownloadProgressEC):
			torrent_entity.get_component(TorrentDownloadProgressEC).wanted = compute_wanted_bitfield(
				self.env, info_hash, torrent_entity.get_component(TorrentInfoEC).info)

		if not torrent_entity.has_component(SaveTorrentEC):
			torrent_entity.add_component(SaveTorrentEC())

		await self.env.event_bus.dispatch_async("action.torrent.files_changed", torrent_entity)

	async def _on_info_added(self, torrent_entity: Entity, _component: TorrentInfoEC) -> None:
		self._create_file_entities(torrent_entity)

	def _create_file_entities(self, torrent_entity: Entity) -> None:
		ds = self.env.data_storage
		info = torrent_entity.get_component(TorrentInfoEC).info
		info_hash = get_info_hash(torrent_entity)

		piece_length = info.piece_length

		# apply per-file selection restored from disk, if any
		selection = {}
		if torrent_entity.has_component(RestoreFileSelectionEC):
			selection = torrent_entity.get_component(RestoreFileSelectionEC).selection
			torrent_entity.remove_component(RestoreFileSelectionEC)

		count = 0
		for index, file in enumerate(info.files):
			wanted, priority = selection.get(index, (True, 0))

			first_piece = file.start // piece_length
			last_piece = (file.start + max(file.length, 1) - 1) // piece_length
			pieces_length = last_piece - first_piece + 1
			path = info.get_file_path(Path(), file).as_posix()

			file_entity = ds.create_entity()
			file_entity.add_component(
				TorrentFileEC(info_hash, index, path, first_piece, pieces_length, file.start, file.length))
			file_entity.add_component(TorrentFileStateEC(bool(wanted), priority))
			count += 1

		logger.info("Created %s file entities for %s", count, info_hash.hex())

		torrent_entity.add_component(TorrentDownloadProgressEC(
			compute_wanted_bitfield(self.env, info_hash, info)))

	async def _on_torrent_remove(self, info_hash: bytes) -> None:
		ds = self.env.data_storage
		for file_entity in list(iterate_files(self.env, info_hash)):
			ds.remove_entity(file_entity)
