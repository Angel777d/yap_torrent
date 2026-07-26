import logging
from pathlib import Path

from angelovich.core.DataStorage import Entity

from yap_torrent.components.file_ec import (
	FilePriority,
	RestoreFileSelectionEC,
	TorrentFileEC,
	TorrentFileStateEC,
)
from yap_torrent.components.torrent_ec import TorrentInfoEC
from yap_torrent.system import System
from yap_torrent.systems import get_info_hash, iterate_files

logger = logging.getLogger(__name__)


class FileSystem(System):
	async def start(self):
		self.add_listener("action.torrent.remove", self._on_torrent_remove)

		# materialize file entities the moment a torrent's metadata becomes known
		# (TorrentInfoEC added by MetainfoSystem, ext-metadata, or a loaded save)
		collection = self.env.data_storage.get_collection(TorrentInfoEC)
		collection.add_listener(collection.EVENT_ADDED, self._on_info_added, self)
		for torrent_entity in collection.entities:
			self._create_file_entities(torrent_entity)

	def close(self):
		collection = self.env.data_storage.get_collection(TorrentInfoEC)
		collection.remove_all_listeners(self)
		self.env.event_bus.remove_all_listeners(scope=self)
		super().close()

	async def _on_info_added(self, torrent_entity: Entity, component: TorrentInfoEC) -> None:
		self._create_file_entities(torrent_entity)

	def _create_file_entities(self, torrent_entity: Entity) -> None:
		ds = self.env.data_storage
		info = torrent_entity.get_component(TorrentInfoEC).info
		info_hash = get_info_hash(torrent_entity)

		# idempotency guard: never materialize a torrent's files twice
		if next(iterate_files(self.env, info_hash), None) is not None:
			return

		piece_length = info.piece_length
		if piece_length <= 0:
			logger.warning("Torrent %s has invalid piece length; no file entities created", info_hash.hex())
			return

		# apply per-file selection restored from disk, if any
		selection = {}
		if torrent_entity.has_component(RestoreFileSelectionEC):
			selection = torrent_entity.get_component(RestoreFileSelectionEC).selection
			torrent_entity.remove_component(RestoreFileSelectionEC)

		count = 0
		for index, file in enumerate(info.files):
			first_piece = file.start // piece_length
			last_piece = (file.start + max(file.length, 1) - 1) // piece_length
			pieces_length = last_piece - first_piece + 1
			path = info.get_file_path(Path(), file).as_posix()

			wanted, priority = selection.get(index, (True, FilePriority.Normal))
			try:
				priority = FilePriority(priority)
			except ValueError:
				priority = FilePriority.Normal

			file_entity = ds.create_entity()
			file_entity.add_component(TorrentFileEC(info_hash, index, path, first_piece, pieces_length))
			file_entity.add_component(TorrentFileStateEC(bool(wanted), priority))
			count += 1

		logger.info("Created %s file entities for %s", count, info_hash.hex())

	async def _on_torrent_remove(self, info_hash: bytes) -> None:
		ds = self.env.data_storage
		for file_entity in list(iterate_files(self.env, info_hash)):
			ds.remove_entity(file_entity)
