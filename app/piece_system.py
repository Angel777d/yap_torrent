import asyncio
import logging
from pathlib import Path

from app import Env, TimeSystem
from app.components.bitfield_ec import BitfieldEC
from app.components.piece_ec import PieceToSaveEC, PieceEC, PiecePendingRemoveEC
from app.components.torrent_ec import TorrentInfoEC, TorrentSaveEC
from app.utils import save_piece
from core.DataStorage import Entity

logger = logging.getLogger(__name__)


class PieceSystem(TimeSystem):

	def __init__(self, env: Env):
		super().__init__(env, 10)
		self.download_path = Path(env.config.download_folder)
		self.download_path.mkdir(parents=True, exist_ok=True)

	async def _update(self, delta_time: float):
		loop = asyncio.get_running_loop()
		await loop.run_in_executor(None, self.save_pieces)
		await self.cleanup()

	def save_pieces(self):
		ds = self.env.data_storage
		for entity in ds.get_collection(PieceToSaveEC).entities:
			entity.remove_component(PieceToSaveEC)

			piece = entity.get_component(PieceEC)
			torrent_entity: Entity = ds.get_collection(TorrentInfoEC).find(piece.info_hash)
			info = torrent_entity.get_component(TorrentInfoEC).info
			save_piece(self.download_path, info, piece.index, piece.data)


			torrent_entity.get_component(BitfieldEC).set_index(piece.index)
			if not torrent_entity.has_component(TorrentSaveEC):
				torrent_entity.add_component(TorrentSaveEC())

				# logs
				downloaded = torrent_entity.get_component(BitfieldEC).have_num * info.pieces.piece_length
				logger.info(f"{min(downloaded, info.size) / info.size * 100:.2f}% progress {info.name}")

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
