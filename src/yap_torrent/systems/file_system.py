import logging
from pathlib import Path

from angelovich.core.DataStorage import Entity

from yap_torrent.components.file_ec import RestoreFileSelectionEC, TorrentFileEC, TorrentFileStateEC
from yap_torrent.components.torrent_ec import TorrentInfoEC, TorrentDownloadProgressEC
from yap_torrent.protocol import InfoHash
from yap_torrent.system import System
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

	async def _on_file_select(self, info_hash: InfoHash, *args):
		torrent_entity = get_torrent_entity(self.env, info_hash)
		if not torrent_entity:
			return

		# TODO: implement file selection change here

		# update wanted bitfield
		torrent_entity.get_component(TorrentDownloadProgressEC).wanted = compute_wanted_bitfield(
			self.env, info_hash, torrent_entity.get_component(TorrentInfoEC).info)

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
			file_entity.add_component(TorrentFileEC(info_hash, index, path, first_piece, pieces_length))
			file_entity.add_component(TorrentFileStateEC(bool(wanted), priority))
			count += 1

		logger.info("Created %s file entities for %s", count, info_hash.hex())

		torrent_entity.add_component(TorrentDownloadProgressEC(
			compute_wanted_bitfield(self.env, info_hash, info)))

	async def _on_torrent_remove(self, info_hash: bytes) -> None:
		ds = self.env.data_storage
		for file_entity in list(iterate_files(self.env, info_hash)):
			ds.remove_entity(file_entity)
