from pathlib import Path

from app import System, Env
from app.components.bitfield_ec import BitfieldEC
from app.components.piece_ec import PieceToSaveEC, PieceEC
from app.components.torrent_ec import TorrentInfoEC, TorrentSaveEC


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
		for entity in ds.get_collection(PieceToSaveEC).entities:
			entity.remove_component(PieceToSaveEC)

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
				print(f"{downloaded / info.size * 100}% progress {info.name}")
