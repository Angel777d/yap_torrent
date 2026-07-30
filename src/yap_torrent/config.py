import json
import logging
import random
import string
from dataclasses import dataclass
from enum import Enum
from pathlib import Path
from typing import Any, Callable, Dict, Optional, Tuple

logger = logging.getLogger(__name__)

_PEER_ID_PREFIX = "-PY0001-"


def generate_peer_id() -> str:
	suffix = "".join(random.choices(string.ascii_letters + string.digits, k=20 - len(_PEER_ID_PREFIX)))
	return _PEER_ID_PREFIX + suffix


class SettingStatus(Enum):
	CHANGED = "changed"
	UNCHANGED = "unchanged"
	UNKNOWN = "unknown"
	RESTART_REQUIRED = "restart_required"
	INVALID = "invalid"


def _as_bool(value: Any) -> bool:
	if isinstance(value, str):
		return value.strip().lower() in ("1", "true", "yes", "on")
	return bool(value)


@dataclass(frozen=True)
class Setting:
	"""One config key: how to read it, what it defaults to, and what acts on it.

	The key is also the attribute name, so `config.download_folder` and the `"download_folder"`
	entry in config.json are the same thing by construction rather than by a mapping kept in
	step by hand.

	`default` may be a callable taking the half-built `Config`, for the keys whose default is
	derived from an earlier one — declaration order is load order.
	"""
	key: str
	cast: Callable[[Any], Any]
	default: Any
	runtime: bool = True  # False: load-only; `set()` refuses it
	restart_required: bool = False
	unenforced: str = ""  # why nothing acts on it; empty means something does

	@property
	def enforced(self) -> bool:
		return not self.unenforced

	def resolve(self, config: "Config", data: Dict[str, Any]) -> Any:
		default = self.default(config) if callable(self.default) else self.default
		return self.cast(data.get(self.key, default))


_BANDWIDTH = "bandwidth limiting is not implemented; the value is stored and reported only"
_QUEUE = "there is no active-torrent queue; the value is stored and reported only"
_PEERS = "connection admission is driven by the queue limits; the value is stored and reported only"
_RATIO = "seeding is never stopped on ratio; the value is stored and reported only"
_INCOMPLETE = "downloads are written straight to download_folder"
_BLOCKLIST = "no blocklist subsystem; no peer is ever filtered"

# Every key core knows, in load order. `runtime=False` means load-only — absent from
# `Config.setting()`, and `set()` reports it UNKNOWN just like a key that does not exist.
SETTINGS: Tuple[Setting, ...] = (
	Setting("data_folder", Path, "data", runtime=False),
	Setting("active_folder", Path, lambda c: c.data_folder / "active", runtime=False),
	Setting("log_path", str, lambda c: f"{c.data_folder}/torrent.log", runtime=False),
	Setting("peers_file", str, lambda c: f"{c.data_folder}/peers.dat", runtime=False),
	Setting("use_log_file", _as_bool, True, runtime=False),
	Setting("disabled_plugins", set, (), runtime=False),
	Setting("dht_peers_per_lookup", int, 20, runtime=False),

	Setting("download_folder", Path, lambda c: c.data_folder / "download"),
	Setting("watch_folder", Path, lambda c: c.data_folder / "watch"),
	Setting("incomplete_folder", Path, lambda c: c.data_folder / "incomplete", unenforced=_INCOMPLETE),
	Setting("incomplete_folder_enabled", _as_bool, False, unenforced=_INCOMPLETE),

	Setting("download_peers_limit", int, 8),
	Setting("upload_peers_limit", int, 4),
	Setting("peer_idle_timeout", float, 30),
	Setting("upload_retry_cooldown", float, 300),
	Setting("block_request_timeout", float, 60),
	Setting("max_cached_pieces", int, 100),
	Setting("piece_cache_ttl", float, 15),

	Setting("port", int, 6889, restart_required=True),
	Setting("dht_port", int, 6999, restart_required=True),
	Setting("dht_enabled", _as_bool, True, restart_required=True),

	Setting("start_added_torrents", _as_bool, True),

	# TODO: enforce these
	Setting("speed_limit_down", int, 0, unenforced=_BANDWIDTH),  # KB/s; 0 is off, no separate flag
	Setting("speed_limit_up", int, 0, unenforced=_BANDWIDTH),
	Setting("seed_ratio_limit", float, 2.0, unenforced=_RATIO),
	Setting("seed_ratio_limited", _as_bool, False, unenforced=_RATIO),
	Setting("download_queue_enabled", _as_bool, False, unenforced=_QUEUE),
	Setting("download_queue_size", int, 0, unenforced=_QUEUE),
	Setting("seed_queue_enabled", _as_bool, False, unenforced=_QUEUE),
	Setting("seed_queue_size", int, 0, unenforced=_QUEUE),
	Setting("max_connections", int, 30, unenforced=_PEERS),
	Setting("peer_limit_per_torrent", int, lambda c: c.max_connections, unenforced=_PEERS),
	Setting("blocklist_enabled", _as_bool, False, unenforced=_BLOCKLIST),
	Setting("blocklist_url", str, "", unenforced=_BLOCKLIST),
)

_RUNTIME_SETTINGS: Dict[str, Setting] = {s.key: s for s in SETTINGS if s.runtime}


class Config:
	DEFAULT_CONFIG = "config.json"

	def __init__(self, path=DEFAULT_CONFIG):
		self._path = path
		data: Dict[str, Any] = {}
		try:
			with open(path, "r") as f:
				data = json.load(f)
		except FileNotFoundError:
			logger.warning(f"Config file not found at {path}. Using default settings.")
		except json.JSONDecodeError:
			logger.warning(f"Config file at {path} is invalid. Using default settings.")

		self._data = data
		for setting in SETTINGS:
			setattr(self, setting.key, setting.resolve(self, data))

		peer_id = data.get("peer_id")
		if not peer_id:
			peer_id = generate_peer_id()
			data["peer_id"] = peer_id
			self._save()
		self.peer_id: bytes = peer_id.encode("latin-1")

	def _save(self):
		# only rewrite a config file that already exists; a missing path means defaults
		if not Path(self._path).exists():
			logger.debug("No config file at %s; keeping the change in memory only", self._path)
			return
		try:
			with open(self._path, "w") as f:
				json.dump(self._data, f, indent=2)
		except OSError as ex:
			logger.warning(f"Could not write config file {self._path}: {ex}")

	@property
	def data(self) -> Dict[str, Any]:
		return self._data

	def get_plugin_config(self, plugin_name: str) -> Dict[str, Any]:
		return self._data.get(plugin_name, {})

	def set_plugin_config(self, plugin_name: str, values: Dict[str, Any]) -> None:
		section = self._data.setdefault(plugin_name, {})
		section.update(values)
		self._save()

	@staticmethod
	def setting(key: str) -> Optional[Setting]:
		return _RUNTIME_SETTINGS.get(key)

	def get(self, key: str) -> Any:
		setting = _RUNTIME_SETTINGS.get(key)
		return getattr(self, setting.key, None) if setting else None

	def set(self, key: str, value: Any) -> SettingStatus:
		setting = _RUNTIME_SETTINGS.get(key)
		if setting is None:
			logger.warning("Ignoring unknown setting '%s'", key)
			return SettingStatus.UNKNOWN

		try:
			cast_value = setting.cast(value)
		except (TypeError, ValueError) as ex:
			logger.warning("Ignoring bad value for '%s': %r (%s)", key, value, ex)
			return SettingStatus.INVALID

		if getattr(self, setting.key, None) == cast_value:
			return SettingStatus.UNCHANGED

		self._data[key] = str(cast_value) if isinstance(cast_value, Path) else value
		self._save()

		if setting.restart_required:
			logger.warning("Setting '%s' is stored but only takes effect after a restart", key)
			return SettingStatus.RESTART_REQUIRED

		setattr(self, setting.key, cast_value)
		if setting.unenforced:
			logger.warning("Setting '%s' is stored but NOT enforced: %s", key, setting.unenforced)
		return SettingStatus.CHANGED
