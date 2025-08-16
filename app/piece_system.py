from pathlib import Path

from app import System, Env
from app.components.piece_ec import PieceToSaveEC, PieceEC
from app.components.torrent_ec import TorrentInfoEC


class PieceSystem(System):

	def __init__(self, env: Env):
		super().__init__(env)
		self.download_path = Path(env.config.download_folder)
		self.download_path.mkdir(parents=True, exist_ok=True)

		self.__timeout = 20  #
		self.__time_left: float = self.__timeout

	async def update(self, delta_time: float):

		self.__time_left -= delta_time
		if self.__time_left > 0:
			return
		self.__time_left += self.__timeout

		ds = self.env.data_storage
		saved_pieces = {}
		for entity in ds.get_collection(PieceToSaveEC).entities:
			entity.remove_component(PieceToSaveEC)

			piece = entity.get_component(PieceEC)
			torrent_entity = ds.get_collection(TorrentInfoEC).find(piece.info_hash)
			torrent_info = torrent_entity.get_component(TorrentInfoEC)
			piece_length = torrent_info.info.pieces.piece_length

			piece_start = piece.index * piece_length
			piece_end = piece_start + piece_length
			for file in torrent_info.info.files:
				file_end = file.start + file.length
				if piece_start >= file_end:
					continue
				if file.start >= piece_end:
					continue

				path = self.download_path
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

			saved_pieces.setdefault(piece.info_hash, []).append(piece.index)

		print(saved_pieces)
