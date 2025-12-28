from pathlib import Path
from typing import Hashable, Dict, Set, Iterable, Generator

from angelovichcore.DataStorage import EntityComponent
from torrent_app.protocol import TorrentInfo
from torrent_app.protocol.structures import PieceBlock


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
	def __init__(self):
		super().__init__()
		self._map: Dict[int, Set[PieceBlock]] = {}
		self._in_progress: Set[PieceBlock] = set()

	def _iter_blocks(self, interested_in: Set[int]) -> Generator[PieceBlock]:
		keys = interested_in.intersection(set(self._map.keys()))
		for key in keys:
			for block in self._map[key]:
				yield block

	def new_pieces(self, interested_in: Set[int]) -> Set[int]:
		return interested_in.difference(self._map.keys())

	def add_blocks(self, blocks: Iterable[PieceBlock]):
		for block in blocks:
			self._map.setdefault(block.index, set()).add(block)

	def has_blocks(self, interested_in: Set[int]) -> bool:
		return any(self._iter_blocks(interested_in))

	def next_block(self, interested_in: Set[int]) -> PieceBlock:
		block = next(self._iter_blocks(interested_in), None)
		self._map[block.index].remove(block)
		self._in_progress.add(block)
		return block

	def complete(self, block: PieceBlock):
		self._in_progress.remove(block)

	def cancel(self, block: PieceBlock):
		if block not in self._in_progress:
			return
		self._in_progress.remove(block)
		self._map.setdefault(block.index, set()).add(block)

	def reset_index(self, index: int):
		self._map.pop(index, None)


class SaveTorrentEC(EntityComponent):
	pass


class ValidateTorrentEC(EntityComponent):
	pass
