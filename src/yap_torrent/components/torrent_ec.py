import logging
import time
from enum import IntEnum
from pathlib import Path
from typing import Any, Dict, Set, Generator, Callable, Optional, Tuple

from angelovich.core.DataStorage import EntityComponent, EntityHashComponent

from yap_torrent.components.peer_ec import PeerConnectionEC
from yap_torrent.protocol import InfoHash
from yap_torrent.protocol import TorrentInfo
from yap_torrent.protocol.structures import PieceBlockInfo, Bitfield

logger = logging.getLogger(__name__)


class TorrentEC(EntityHashComponent):
	__index = 0

	def __init__(self, info_hash: InfoHash) -> None:
		super().__init__()
		TorrentEC.__index += 1
		self.index: int = TorrentEC.__index
		self.info_hash: InfoHash = info_hash
		self.bitfield: Bitfield = Bitfield()

	def __hash__(self):
		return hash(self.info_hash)


class TorrentInfoEC(EntityComponent):
	def __init__(self, torrent_info: TorrentInfo) -> None:
		super().__init__()
		self.info: TorrentInfo = torrent_info


class TorrentPathEC(EntityComponent):
	def __init__(self, path: Path) -> None:
		super().__init__()
		self.root_path: Path = path


class TorrentRateEC(EntityComponent):
	"""Sampled transfer rates for a torrent, in bytes/sec.

	Derived state, rebuilt from the peers on every sample — never accumulated here, and
	never persisted: a rate describes a moment, and a stale one read back from disk would
	claim a transfer that is not happening.
	"""

	def __init__(self) -> None:
		super().__init__()
		self.down_rate: float = 0.0
		self.up_rate: float = 0.0


class TorrentState(IntEnum):
	Active = 1
	Inactive = 2


class TorrentStatsEC(EntityComponent):
	"""Totals and dates for a torrent. Everything here survives a restart.

	The dates are **wall-clock epoch seconds**, not `time.monotonic()`: they are shown to
	users and written to disk, and a monotonic stamp means nothing in the next process.
	"""

	def __init__(self, **kwargs) -> None:
		super().__init__()

		self._uploaded: int = kwargs.get("uploaded", 0)
		self._downloaded: int = kwargs.get("downloaded", 0)
		self._session_downloaded = 0
		self._session_uploaded = 0

		self.state: TorrentState = TorrentState(kwargs.get("state", TorrentState.Active))

		# a save written before these existed has no added_date, so it dates from the
		# first run that understands them rather than from the epoch
		self.added_date: float = kwargs.get("added_date") or time.time()
		self.started_date: float = kwargs.get("started_date", 0.0)
		self.done_date: float = kwargs.get("done_date", 0.0)
		self.activity_date: float = kwargs.get("activity_date", 0.0)

	def export(self) -> Dict[str, Any]:
		return {
			"uploaded": self.uploaded,
			"downloaded": self.downloaded,
			"state": self.state.value,
			"added_date": self.added_date,
			"started_date": self.started_date,
			"done_date": self.done_date,
			"activity_date": self.activity_date,
		}

	def touch_activity(self) -> None:
		self.activity_date = time.time()

	def update_uploaded(self, length: int) -> None:
		self._uploaded += length
		self._session_uploaded += length
		self.touch_activity()

	def update_downloaded(self, length: int) -> None:
		self._downloaded += length
		self._session_downloaded += length
		self.touch_activity()

	@property
	def uploaded(self) -> int:
		return self._uploaded

	@property
	def downloaded(self) -> int:
		return self._downloaded

	@property
	def session_uploaded(self) -> float:
		return self._session_uploaded

	@property
	def session_downloaded(self) -> float:
		return self._session_downloaded


class TorrentDownloadEC(EntityComponent):
	class InProgress:
		MAX_DOWNLOADS_PER_PEER = 10

		def __init__(self):
			self._blocks_to_peers: Dict[PieceBlockInfo, Set[PeerConnectionEC]] = {}
			self._peers_to_block: Dict[PeerConnectionEC, Set[PieceBlockInfo]] = {}

		def add(self, block: PieceBlockInfo, peer: PeerConnectionEC):
			self._blocks_to_peers.setdefault(block, set()).add(peer)
			self._peers_to_block.setdefault(peer, set()).add(block)

		def remove_block(self, block) -> Set[PeerConnectionEC]:
			peers = self._blocks_to_peers.pop(block, set())
			for peer in peers:
				self._peers_to_block[peer].remove(block)
			return peers

		def remove_peer(self, peer_hash) -> Set[PieceBlockInfo]:
			blocks = self._peers_to_block.pop(peer_hash, set())
			for block in blocks:
				self._blocks_to_peers[block].remove(peer_hash)
			return blocks

		def get_endgame_block(self, interested_in: Set[int], peer: PeerConnectionEC) -> Optional[PieceBlockInfo]:
			for block in self._blocks_to_peers:
				if block.index in interested_in and peer not in self._blocks_to_peers[block]:
					return block
			return None

		def has_free_slot(self, peer: PeerConnectionEC) -> bool:
			return len(self._peers_to_block.get(peer, set())) < self.MAX_DOWNLOADS_PER_PEER

	class PieceData:
		def __init__(self, size: int):
			self._size = size
			self._downloaded = 0
			self.data = bytearray(size)

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

		self._blocks_queue: Set[PieceBlockInfo] = set()
		self._pieces: Dict[int, TorrentDownloadEC.PieceData] = {}

		self._in_progress: TorrentDownloadEC.InProgress = TorrentDownloadEC.InProgress()

		super().__init__()

	def _find_next_block(self, interested_in: Set[int]) -> Optional[PieceBlockInfo]:
		# looking in already requested blocks
		for block in self._blocks_queue:
			if block.index in interested_in:
				return block

		# try to add a next piece
		new_blocks = self._add_blocks(interested_in)
		for block in new_blocks:
			return block

		return None

	def _get_piece(self, index: int) -> "TorrentDownloadEC.PieceData":
		if index not in self._pieces:
			self._register_piece(index)
		return self._pieces[index]

	def _register_piece(self, index: int) -> Set[PieceBlockInfo]:
		piece_info = self._info.get_piece_info(index)

		# register a new piece
		self._pieces[index] = TorrentDownloadEC.PieceData(piece_info.size)

		# add a new piece to the blocks_manager
		new_blocks = piece_info.create_blocks()
		self._blocks_queue.update(new_blocks)
		return new_blocks

	def _add_blocks(self, interested_in: Set[int]) -> Set[PieceBlockInfo]:
		# check there are any other pieces to download
		new_keys = interested_in.difference(self._pieces.keys())
		if not new_keys:
			return set()

		# find a next piece index
		index = self._find_next_piece(new_keys)

		return self._register_piece(index)

	def request_blocks(self, interested_in: Set[int], peer: PeerConnectionEC) -> Generator[PieceBlockInfo]:

		# check this peer can have more
		while self._in_progress.has_free_slot(peer):
			# Attempt to get block from peers
			block = self._find_next_block(interested_in)

			# Endgame starts here. Get block from already in progress blocks
			if not block:
				block = self._in_progress.get_endgame_block(interested_in, peer)

			# Give up. There is nothing to download
			if not block:
				return

			self._blocks_queue.discard(block)
			self._in_progress.add(block, peer)

			logger.debug("%s requested by %s", block, peer)
			yield block

	def set_block_data(self, block: PieceBlockInfo, data: bytes, peer: PeerConnectionEC) -> Tuple[
		bool, Set[PeerConnectionEC]]:
		piece = self._get_piece(block.index)
		piece.add_block(block, data)

		# clear download queue
		peers_to_notify = self._in_progress.remove_block(block)

		if not peers_to_notify:
			logger.info("Got unexpected %s from peer %s", block, peer)
			# Block just downloaded. Suspect it is in the queue. Remove it
			self._blocks_queue.discard(block)

		# remove own peer
		peers_to_notify.discard(peer)
		return piece.is_full(), peers_to_notify

	def pop_piece_data(self, index: int) -> bytes:
		piece = self._pieces.pop(index, None)
		return piece.data if piece else bytes()

	def cancel(self, peer: PeerConnectionEC):
		logger.debug("%s cleaned up.", peer)
		blocks = self._in_progress.remove_peer(peer)
		self._blocks_queue.update(blocks)


class SaveTorrentEC(EntityComponent):
	pass


class ValidateTorrentEC(EntityComponent):
	pass


# Torrent, selected to download now
class TorrentDownloadProgressEC(EntityComponent):
	def __init__(self, wanted: Bitfield) -> None:
		super().__init__()
		self.wanted: Bitfield = wanted


class TorrentPriorityEC(EntityComponent):
	"""Download-order priority (lower is served first, like	a queue position)."""

	def __init__(self, priority: int = 0) -> None:
		super().__init__()
		self.priority: int = priority

