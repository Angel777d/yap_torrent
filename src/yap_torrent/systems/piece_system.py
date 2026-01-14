import asyncio
import logging
from pathlib import Path

from angelovich.core.DataStorage import Entity

from yap_torrent.components.piece_ec import PieceToSaveEC, PieceEC, PiecePendingRemoveEC
from yap_torrent.components.torrent_ec import TorrentInfoEC, SaveTorrentEC, TorrentEC
from yap_torrent.env import Env
from yap_torrent.system import TimeSystem
from yap_torrent.systems import calculate_downloaded, get_torrent_entity
from yap_torrent.utils import save_piece

logger = logging.getLogger(__name__)


async def _on_piece_complete(_: Entity, piece_entity: Entity):
	piece_entity.add_component(PieceToSaveEC())


class PieceSystem(TimeSystem):

	def __init__(self, env: Env):
		super().__init__(env, 10)
		self.download_path = Path(env.config.download_folder)
		self.download_path.mkdir(parents=True, exist_ok=True)

	async def start(self):
		self.env.event_bus.add_listener("piece.complete", _on_piece_complete, scope=self)
		self.env.event_bus.add_listener("action.torrent.complete", self.__on_torrent_complete, scope=self)
		self.env.event_bus.add_listener("action.torrent.remove", self._on_torrent_remove, scope=self)

	def close(self) -> None:
		super().close()
		self.env.event_bus.remove_all_listeners(scope=self)

	async def __on_torrent_complete(self, _: Entity):
		await self._save()

	async def _on_torrent_remove(self, info_hash: bytes):
		torrent_entity = get_torrent_entity(self.env, info_hash)

		to_remove = (e for e in self.env.data_storage.get_collection(PieceEC).entities if
		             e.get_component(PieceEC).info_hash == info_hash)
		for entity in to_remove:
			self.env.data_storage.remove_entity(entity)

		if torrent_entity.has_component(SaveTorrentEC):
			torrent_entity.remove_component(SaveTorrentEC)

	async def _update(self, delta_time: float):
		await self._save()
		await self.cleanup()

	async def _save(self):
		loop = asyncio.get_running_loop()
		await loop.run_in_executor(None, self.save_pieces)

	def save_pieces(self):
		ds = self.env.data_storage
		updated_torrents = set()
		for piece_entity in ds.get_collection(PieceToSaveEC).entities:
			piece_entity.remove_component(PieceToSaveEC)

			piece: PieceEC = piece_entity.get_component(PieceEC)
			updated_torrents.add(piece.info_hash)
			torrent_entity: Entity = ds.get_collection(TorrentEC).find(piece.info_hash)
			torrent_info = torrent_entity.get_component(TorrentInfoEC).info
			save_piece(self.download_path, torrent_info, piece.info.index, piece.data)

		for info_hash in updated_torrents:
			torrent_entity: Entity = ds.get_collection(TorrentEC).find(info_hash)
			torrent_info = torrent_entity.get_component(TorrentInfoEC).info
			if not torrent_entity.has_component(SaveTorrentEC):
				torrent_entity.add_component(SaveTorrentEC())

			# logs
			logger.info(f"{calculate_downloaded(torrent_entity):.2%} progress {torrent_info.name}")

	async def cleanup(self):
		MAX_PIECES = 100  # TODO: move to config

		ds = self.env.data_storage
		all_pieces = len(ds.get_collection(PieceEC))
		if all_pieces <= MAX_PIECES:
			return

		collection = ds.get_collection(PiecePendingRemoveEC).entities
		# filter pieces can be removed
		collection = [e for e in collection if e.get_component(PieceEC).completed and e.get_component(
			PiecePendingRemoveEC).can_remove() and not e.has_component(PieceToSaveEC)]
		collection.sort(key=lambda e: e.get_component(PiecePendingRemoveEC).last_update)

		to_remove = collection[:all_pieces - MAX_PIECES]
		logger.debug(f"cleanup pieces: {len(to_remove)} removed")
		for entity in to_remove:
			ds.remove_entity(entity)
