import time
from enum import IntEnum
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Set

from angelovich.core.DataStorage import EntityComponent, EntityHashComponent

from yap_torrent.protocol import InfoHash
from yap_torrent.protocol import TorrentInfo
from yap_torrent.protocol.structures import Bitfield


class TorrentEC(EntityHashComponent):
	def __init__(self, info_hash: InfoHash, display_name: str = "") -> None:
		super().__init__()
		self.info_hash: InfoHash = info_hash
		self.bitfield: Bitfield = Bitfield()
		# What to call this torrent until its metadata arrives — a magnet's "dn", when it
		# carried one. `TorrentInfoEC.info.name` is the real name and wins the moment it
		# exists, so this is never consulted again after that; it lives here rather than in
		# its own component because it is part of what identifies a torrent to a person,
		# and it has to survive a restart the same way the info_hash does.
		self.display_name: str = display_name

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


class TorrentCustomDataEC(EntityComponent):
	"""Whatever plugins want to keep on a torrent, one entry per plugin name.

	Core's entire involvement is writing this out with the torrent and reading it back at
	startup. The values are `Any`, nothing here looks inside them, and nothing here decides
	what they mean — that is what lets a concept only one plugin has (Transmission's
	labels, say) outlive a restart without becoming a core component. The one requirement
	is that a value pickles, since `LocalDataSystem` saves it with everything else.

	A plugin's *settings* still belong in its `config.json` block; this is for the
	per-torrent state that has nowhere else to live.

	Reach it through `get_custom_data` / `set_custom_data` — the latter also asks for the
	save, which is the part that is easy to forget.
	"""

	def __init__(self, data: Optional[Dict[str, Any]] = None) -> None:
		super().__init__()
		self.data: Dict[str, Any] = dict(data or {})


class TorrentLimitsEC(EntityComponent):
	"""Per-torrent bandwidth and seeding preferences. Stored, not yet enforced.

	Core state, not a plugin's: these are what a download/upload speed cap will read once
	the transfer path enforces them, alongside the global `speed_limit_*` in `Config`.
	"""

	def __init__(self, **kwargs) -> None:
		super().__init__()
		self.download_limit: int = int(kwargs.get("download_limit", 0))  # KB/s
		self.download_limited: bool = bool(kwargs.get("download_limited", False))
		self.upload_limit: int = int(kwargs.get("upload_limit", 0))  # KB/s
		self.upload_limited: bool = bool(kwargs.get("upload_limited", False))
		self.honors_session_limits: bool = bool(kwargs.get("honors_session_limits", True))
		self.seed_ratio_limit: float = float(kwargs.get("seed_ratio_limit", 0.0))
		self.seed_ratio_mode: int = int(kwargs.get("seed_ratio_mode", 0))  # 0 global, 1 single, 2 unlimited
		self.peer_limit: int = int(kwargs.get("peer_limit", 0))
		self.bandwidth_priority: int = int(kwargs.get("bandwidth_priority", 0))  # -1 low, 0 normal, 1 high

	FIELDS = (
		"download_limit", "download_limited", "upload_limit", "upload_limited",
		"honors_session_limits", "seed_ratio_limit", "seed_ratio_mode", "peer_limit",
		"bandwidth_priority",
	)

	def export(self) -> Dict[str, Any]:
		return {name: getattr(self, name) for name in self.FIELDS}


class TorrentRateEC(EntityComponent):
	"""Sampled transfer rates in bytes/sec. Derived, never persisted."""

	def __init__(self) -> None:
		super().__init__()
		self.down_rate: float = 0.0
		self.up_rate: float = 0.0


class TorrentPieceAvailabilityEC(EntityComponent):
	def __init__(self) -> None:
		super().__init__()
		self._counts: Dict[int, int] = {}
		self._order: List[int] = []
		self._unsorted = True
		self._needs_rebuild = True

	@property
	def needs_rebuild(self) -> bool:
		"""Whether the counts describe a swarm that has since changed."""
		return self._needs_rebuild

	def invalidate(self) -> None:
		"""The set of peers changed; the next read has to recount."""
		self._needs_rebuild = True

	def rebuild(self, holdings: Iterable[Iterable[int]], wanted: Set[int]) -> None:
		"""Recount from scratch: how many of `holdings` cover each piece in `wanted`."""
		counts = dict.fromkeys(wanted, 0)
		for held in holdings:
			for index in held:
				if index in counts:
					counts[index] += 1

		self._counts = counts
		self._unsorted = True
		self._needs_rebuild = False

	def add_have(self, index: int) -> None:
		"""One more peer announced this piece. Pieces we do not want are not tracked."""
		if index in self._counts:
			self._counts[index] += 1
			self._unsorted = True

	def drop(self, index: int) -> None:
		"""Stop offering this piece — we finished it, or it is no longer wanted."""
		if self._counts.pop(index, None) is not None:
			self._unsorted = True

	def count(self, index: int) -> int:
		"""How many peers hold this piece; 0 for one nobody has or we no longer want."""
		return self._counts.get(index, 0)

	def rarest_first(self) -> List[int]:
		"""Wanted, obtainable pieces, fewest holders first. Sorted on demand, once."""
		if self._unsorted:
			self._order = sorted((index for index, held_by in self._counts.items() if held_by > 0),
			                     key=self._counts.__getitem__)
			self._unsorted = False
		return self._order

	def rarest_of(self, candidates: Set[int]) -> int:
		"""The rarest of `candidates`, or any of them if none has been counted yet.

		Walking the shared order beats scoring the candidates: for a peer holding most of
		the torrent the first entry usually matches, and the walk is shared across peers
		rather than repeated per peer.
		"""
		for index in self.rarest_first():
			if index in candidates:
				return index
		return next(iter(candidates))


class TorrentState(IntEnum):
	Active = 1
	Inactive = 2


class TorrentStatsEC(EntityComponent):
	"""Totals and dates for a torrent. Dates are wall-clock epoch seconds."""

	def __init__(self, **kwargs) -> None:
		super().__init__()

		self._uploaded: int = kwargs.get("uploaded", 0)
		self._downloaded: int = kwargs.get("downloaded", 0)

		self.state: TorrentState = TorrentState(kwargs.get("state", TorrentState.Active))

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
		self.touch_activity()

	def update_downloaded(self, length: int) -> None:
		self._downloaded += length
		self.touch_activity()

	@property
	def uploaded(self) -> int:
		return self._uploaded

	@property
	def downloaded(self) -> int:
		return self._downloaded


class SaveTorrentEC(EntityComponent):
	pass


class ValidateTorrentEC(EntityComponent):
	pass


# Torrent, selected to download now
class TorrentDownloadProgressEC(EntityComponent):
	def __init__(self, wanted: Bitfield) -> None:
		super().__init__()
		self.wanted: Bitfield = wanted


class TorrentQueuePositionEC(EntityComponent):
	"""Queue position: a dense 0..n-1 ordinal, lowest served first. TorrentSystem owns it."""

	def __init__(self, position: int = 0) -> None:
		super().__init__()
		self.position: int = position

