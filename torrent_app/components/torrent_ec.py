import logging
from pathlib import Path
from typing import Hashable, Dict, Set, Generator, Callable

from angelovichcore.DataStorage import EntityComponent
from torrent_app.protocol import TorrentInfo
from torrent_app.protocol.structures import PieceBlockInfo, PieceInfo
from torrent_app.utils import check_hash

logger = logging.getLogger(__name__)


class TorrentHashEC(EntityComponent):
	def __init__(self, info_hash: bytes) -> None:
		super().__init__()
		self.info_hash: bytes = info_hash

	@classmethod
	def is_hashable(cls) -> bool:
		return True

	def get_hash(self) -> Hashable:
		return self.info_hash


class TorrentInfoEC(EntityComponent):
	def __init__(self, torrent_info: TorrentInfo) -> None:
		super().__init__()
		self.info: TorrentInfo = torrent_info


class TorrentPathEC(EntityComponent):
	def __init__(self, path: Path) -> None:
		super().__init__()
		self.root_path: Path = path


class TorrentStatsEC(EntityComponent):
	def __init__(self, **kwargs) -> None:
		super().__init__()

		self.uploaded = kwargs.get("uploaded", 0)
		self.downloaded = kwargs.get("downloaded", 0)

	def export(self) -> Dict[str, int]:
		return {
			"uploaded": self.uploaded,
			"downloaded": self.downloaded
		}

	def update_uploaded(self, length: int) -> None:
		self.uploaded += length

	def update_downloaded(self, length: int) -> None:
		self.downloaded += length


class TorrentDownloadEC(EntityComponent):
	class PieceData:
		def __init__(self, info: PieceInfo):
			self._size = info.size
			self._downloaded = 0
			self.data = bytearray(info.size)

			self._blocks: Set[int] = set()

		def add_block(self, block: PieceBlockInfo, data: bytes):
			if block.begin in self._blocks:
				return
			self._blocks.add(block.begin)

			self.data[block.begin:block.begin + block.length] = data
			self._downloaded += block.length

		def is_full(self) -> bool:
			return self._size == self._downloaded

	def __init__(self, info: TorrentInfo, find_next_piece: Callable[[Set[int]], int]):
		self._info: TorrentInfo = info
		self._find_next_piece: Callable[[Set[int]], int] = find_next_piece

		self._blocks: Dict[int, Set[PieceBlockInfo]] = {}
		self._pieces: Dict[int, TorrentDownloadEC.PieceData] = {}

		self._peers: Dict[Hashable, Set[PieceBlockInfo]] = {}

		super().__init__()

	def _iter_blocks(self, interested_in: Set[int]) -> Generator[PieceBlockInfo]:
		keys = interested_in.intersection(set(self._blocks.keys()))
		for key in keys:
			for block in self._blocks[key]:
				yield block

	def _add_blocks(self, interested_in: Set[int]):
		# check there are any other pieces to download
		new_pieces = interested_in.difference(self._blocks.keys())
		if not new_pieces:
			return False

		# add a new piece to the blocks_manager
		index = self._find_next_piece(new_pieces)

		self._blocks.setdefault(index, set()).update(self._info.create_blocks(index))
		self._pieces[index] = TorrentDownloadEC.PieceData(self._info.get_piece_info(index))

		return True

	def request_blocks(self, interested_in: Set[int], peer_hash: Hashable) -> Generator[PieceBlockInfo]:
		# TODO: move from here
		max_downloads_per_peer = 10

		while True:
			# check this peer can have more
			if len(self._peers.get(peer_hash, set())) >= max_downloads_per_peer:
				return

			# attempt to get block from peers
			block = next(self._iter_blocks(interested_in), None)
			if not block:
				# try to add a new piece
				if not self._add_blocks(interested_in):
					return
				# second attempt to get from a new-added piece
				block = next(self._iter_blocks(interested_in), None)

			self._peers.setdefault(peer_hash, set()).add(block)
			self._blocks[block.index].remove(block)
			yield block

	def set_block_data(self, block: PieceBlockInfo, data: bytes, peer_hash: Hashable):
		if block.index in self._pieces:
			self._pieces[block.index].add_block(block, data)
		else:
			# unexpected block. maybe already downloaded
			logger.debug(f"Unexpected block {block}.")

		# clear download queue
		if block in self._peers[peer_hash]:
			self._peers[peer_hash].remove(block)

	def get_piece_data(self, index: int) -> bytes:
		if index not in self._pieces:
			logger.error(f"Invalid piece index: {index}.")
			return bytes()

		piece = self._pieces[index]
		if not piece.is_full():
			logger.error("Try to pull data from not full piece.")
			return bytes()

		result = bytes(piece.data)
		del self._pieces[index]

		if not check_hash(result, self._info.get_piece_hash(index)):
			logger.error(f"Piece {index} is invalid.")
			# clean blocks to download again
			self._blocks.pop(index, None)
			return bytes()

		return result

	def is_completed(self, index: int) -> bool:
		if index not in self._pieces:
			return False
		return self._pieces[index].is_full()

	def cancel(self, peer_hash: Hashable):
		logger.debug(f"{peer_hash} cleaned up.")

		# return blocks to the queue
		for block in self._peers.get(peer_hash, set()):
			if block.index not in self._blocks:
				logger.error(f"{block} is not in blocks manager!")
				continue
			self._blocks[block.index].add(block)


class SaveTorrentEC(EntityComponent):
	pass


class ValidateTorrentEC(EntityComponent):
	pass
