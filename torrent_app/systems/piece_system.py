import asyncio
import logging
from pathlib import Path

from angelovichcore.DataStorage import Entity
from torrent_app import Env, TimeSystem
from torrent_app.components.bitfield_ec import BitfieldEC
from torrent_app.components.piece_ec import PieceToSaveEC, PieceEC, PiecePendingRemoveEC
from torrent_app.components.torrent_ec import TorrentInfoEC, SaveTorrentEC, TorrentHashEC
from torrent_app.utils import save_piece

logger = logging.getLogger(__name__)


class PieceSystem(TimeSystem):

	def __init__(self, env: Env):
		super().__init__(env, 10)
		self.download_path = Path(env.config.download_folder)
		self.download_path.mkdir(parents=True, exist_ok=True)

	async def start(self):
		self.env.event_bus.add_listener("piece.complete", self.__on_piece_complete, scope=self)

	def close(self) -> None:
		super().close()
		self.env.event_bus.remove_all_listeners(scope=self)

	async def __on_piece_complete(self, _: Entity, piece_entity: Entity):
		piece_entity.add_component(PieceToSaveEC())

	async def _update(self, delta_time: float):
		loop = asyncio.get_running_loop()
		await loop.run_in_executor(None, self.save_pieces)
		await self.cleanup()

	def save_pieces(self):
		ds = self.env.data_storage
		updated_torrents = set()
		for piece_entity in ds.get_collection(PieceToSaveEC).entities:
			piece_entity.remove_component(PieceToSaveEC)

			piece: PieceEC = piece_entity.get_component(PieceEC)
			updated_torrents.add(piece.info_hash)
			torrent_entity: Entity = ds.get_collection(TorrentHashEC).find(piece.info_hash)
			torrent_info = torrent_entity.get_component(TorrentInfoEC).info
			save_piece(self.download_path, torrent_info, piece.info.index, piece.data)

		for info_hash in updated_torrents:
			torrent_entity: Entity = ds.get_collection(TorrentHashEC).find(info_hash)
			torrent_info = torrent_entity.get_component(TorrentInfoEC).info
			if not torrent_entity.has_component(SaveTorrentEC):
				torrent_entity.add_component(SaveTorrentEC())

			# logs
			have_num = torrent_entity.get_component(BitfieldEC).have_num
			logger.info(f"{torrent_info.calculate_downloaded(have_num):.2%} progress {torrent_info.name}")

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
