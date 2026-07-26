import logging
from asyncio import Task
from pathlib import Path
from typing import List, Tuple, Set

from angelovich.core.DataStorage import Entity

from yap_torrent.components.piece_ec import PieceEC, PiecePendingRemoveEC
from yap_torrent.components.torrent_ec import TorrentInfoEC, SaveTorrentEC, TorrentEC
from yap_torrent.env import Env
from yap_torrent.protocol import InfoHash
from yap_torrent.protocol import TorrentInfo
from yap_torrent.system import TimeSystem
from yap_torrent.systems import calculate_downloaded, get_torrent_entity
from yap_torrent.utils import save_piece, execute_in_pool

logger = logging.getLogger(__name__)

PieceToSave = tuple[InfoHash, TorrentInfo, int, bytes]


class PieceSystem(TimeSystem):

	def __init__(self, env: Env):
		super().__init__(env, 10)
		self.download_path = Path(env.config.download_folder)
		self.download_path.mkdir(parents=True, exist_ok=True)

		self._to_save: List[PieceToSave] = []

	async def start(self):
		self.add_listener("piece.complete", self._on_piece_complete)
		self.add_listener("action.torrent.complete", self.__on_torrent_complete)
		self.add_listener("action.torrent.remove", self._on_torrent_remove)

	async def __on_torrent_complete(self, _: Entity):
		await self._save()

	async def _on_torrent_remove(self, info_hash: bytes):
		to_remove = (e for e in self.env.data_storage.get_collection(PieceEC).entities if
		             e.get_component(PieceEC).info_hash == info_hash)
		for entity in to_remove:
			self.env.data_storage.remove_entity(entity)

	async def _update(self, delta_time: float):
		await self._save()
		await self._cleanup()

	async def _cleanup(self):
		MAX_PIECES = 100  # TODO: move to config

		ds = self.env.data_storage
		all_pieces = len(ds.get_collection(PieceEC))
		if all_pieces <= MAX_PIECES:
			return

		# filter pieces can be removed
		collection = sorted((
			e for e in ds.get_collection(PiecePendingRemoveEC)
			if e.get_component(PieceEC).completed
			   and e.get_component(PiecePendingRemoveEC).can_remove()),
			key=lambda e: e.get_component(PiecePendingRemoveEC).last_update)

		to_remove = collection[:all_pieces - MAX_PIECES]
		logger.debug(f"cleanup pieces: {len(to_remove)} removed")
		for entity in to_remove:
			ds.remove_entity(entity)

	async def _on_piece_complete(self, torrent_entity: Entity, piece_entity: Entity):
		info_hash = torrent_entity.get_component(TorrentEC).info_hash
		info = torrent_entity.get_component(TorrentInfoEC).info
		piece = piece_entity.get_component(PieceEC)
		self._to_save.append((info_hash, info, piece.info.index, piece.data))

	async def _save(self):
		if not self._to_save:
			return

		chunks = self._to_save.copy()
		self._to_save.clear()

		def update_torrents(_task: Task[Set[InfoHash]]):
			if _task.cancelled():
				return
			for info_hash in _task.result():
				torrent_entity = get_torrent_entity(self.env, info_hash)
				if torrent_entity and not torrent_entity.has_component(SaveTorrentEC):
					torrent_entity.add_component(SaveTorrentEC())
					logger.info(
						f"{calculate_downloaded(torrent_entity):.2%} progress {torrent_entity.get_component(TorrentInfoEC).info.name}")

		self.add_task(execute_in_pool(_save_pieces, self.download_path, chunks), update_torrents)


def _save_pieces(download_path: Path, chunks: List[Tuple[InfoHash, TorrentInfo, int, bytes]]):
	for _, torrent_info, index, piece_data in chunks:
		save_piece(download_path, torrent_info, index, piece_data)

	return set(info_hash for info_hash, _, _, _ in chunks)
