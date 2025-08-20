import asyncio
import logging
from pathlib import Path

from app import Env, TimeSystem
from app.components.bitfield_ec import BitfieldEC
from app.components.piece_ec import PieceToSaveEC, PieceEC
from app.components.torrent_ec import TorrentInfoEC, TorrentSaveEC

logger = logging.getLogger(__name__)


class PieceSystem(TimeSystem):

	def __init__(self, env: Env):
		super().__init__(env, 10)
		self.download_path = Path(env.config.download_folder)
		self.download_path.mkdir(parents=True, exist_ok=True)

	async def _update(self, delta_time: float):
		loop = asyncio.get_running_loop()
		await loop.run_in_executor(None, self.save_pieces)

	def save_pieces(self):
		ds = self.env.data_storage
		for entity in ds.get_collection(PieceToSaveEC).entities:
			piece = entity.get_component(PieceEC)
			torrent_entity = ds.get_collection(TorrentInfoEC).find(piece.info_hash)
			info = torrent_entity.get_component(TorrentInfoEC).info
			piece_length = info.pieces.piece_length

			piece_start = piece.index * piece_length
			piece_end = piece_start + piece_length
			for file in info.files:
				file_end = file.start + file.length
				if piece_start >= file_end:
					continue
				if file.start >= piece_end:
					continue

				path = self.download_path
				# add folder for multifile torrent
				if info.is_multifile:
					path = path.joinpath(info.name)
				for file_path in file.path:
					path = path.joinpath(file_path)
				path.parent.mkdir(parents=True, exist_ok=True)

				start_pos = max(piece_start, file.start)
				end_pos = min(piece_end, file_end)
				buffer = piece.data[start_pos % piece_length:end_pos % piece_length]

				offset = start_pos - file.start

				with open(path, "r+b" if path.exists() else "wb") as f:
					f.seek(offset)
					f.write(buffer)

			torrent_entity.get_component(BitfieldEC).set_index(piece.index)
			if not torrent_entity.has_component(TorrentSaveEC):
				torrent_entity.add_component(TorrentSaveEC())

				# logs
				downloaded = torrent_entity.get_component(BitfieldEC).have_num * info.pieces.piece_length
				logger.info(f"{downloaded / info.size * 100:.2f}% progress {info.name}")

		# cleanup
		ds.clear_collection(PieceToSaveEC)
