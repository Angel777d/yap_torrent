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


@dataclass(frozen=True)
class Setting:
	key: str
	attr: str
	cast: Callable[[Any], Any]
	enforced: bool = True
	restart_required: bool = False
	note: str = ""


def _as_bool(value: Any) -> bool:
	if isinstance(value, str):
		return value.strip().lower() in ("1", "true", "yes", "on")
	return bool(value)


_NOT_ENFORCED_BANDWIDTH = "bandwidth limiting is not implemented; the value is stored and reported only"
_NOT_ENFORCED_QUEUE = "there is no active-torrent queue; the value is stored and reported only"
_NOT_ENFORCED_PEERS = "connection admission is driven by the queue limits; the value is stored and reported only"

# Keys that may be changed at runtime. Keys absent from here are load-only.
SETTINGS: Tuple[Setting, ...] = (
	Setting("download_folder", "download_folder", Path),
	Setting("incomplete_folder", "incomplete_folder", Path, enforced=False,
	        note="downloads are written straight to download_folder"),
	Setting("incomplete_folder_enabled", "incomplete_folder_enabled", _as_bool, enforced=False,
	        note="downloads are written straight to download_folder"),
	Setting("watch_folder", "watch_folder", Path),

	Setting("download_peers_limit", "download_peers_limit", int),
	Setting("upload_peers_limit", "upload_peers_limit", int),
	Setting("peer_idle_timeout", "peer_idle_timeout", float),
	Setting("upload_retry_cooldown", "upload_retry_cooldown", float),
	Setting("block_request_timeout", "block_request_timeout", float),
	Setting("max_cached_pieces", "max_cached_pieces", int),
	Setting("piece_cache_ttl", "piece_cache_ttl", float),

	Setting("port", "port", int, restart_required=True),
	Setting("dht_port", "dht_port", int, restart_required=True),

	# TODO: enforce these
	Setting("speed_limit_down", "speed_limit_down", int, enforced=False, note=_NOT_ENFORCED_BANDWIDTH),
	Setting("speed_limit_up", "speed_limit_up", int, enforced=False, note=_NOT_ENFORCED_BANDWIDTH),

	Setting("seed_ratio_limit", "seed_ratio_limit", float, enforced=False,
	        note="seeding is never stopped on ratio; the value is stored and reported only"),
	Setting("seed_ratio_limited", "seed_ratio_limited", _as_bool, enforced=False,
	        note="seeding is never stopped on ratio; the value is stored and reported only"),

	Setting("download_queue_enabled", "download_queue_enabled", _as_bool, enforced=False, note=_NOT_ENFORCED_QUEUE),
	Setting("download_queue_size", "download_queue_size", int, enforced=False, note=_NOT_ENFORCED_QUEUE),
	Setting("seed_queue_enabled", "seed_queue_enabled", _as_bool, enforced=False, note=_NOT_ENFORCED_QUEUE),
	Setting("seed_queue_size", "seed_queue_size", int, enforced=False, note=_NOT_ENFORCED_QUEUE),

	Setting("max_connections", "max_connections", int, enforced=False, note=_NOT_ENFORCED_PEERS),
	Setting("peer_limit_per_torrent", "peer_limit_per_torrent", int, enforced=False, note=_NOT_ENFORCED_PEERS),

	Setting("blocklist_enabled", "blocklist_enabled", _as_bool, enforced=False,
	        note="no blocklist subsystem; no peer is ever filtered"),
	Setting("blocklist_url", "blocklist_url", str, enforced=False,
	        note="no blocklist subsystem; no peer is ever filtered"),

	Setting("start_added_torrents", "start_added_torrents", _as_bool),
	Setting("dht_enabled", "dht_enabled", _as_bool, restart_required=True),
)

_SETTINGS_BY_KEY: Dict[str, Setting] = {setting.key: setting for setting in SETTINGS}


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

		peer_id = data.get("peer_id")
		if not peer_id:
			peer_id = generate_peer_id()
			data["peer_id"] = peer_id
			self._data = data
			if Path(path).exists():
				self._save()
		self.peer_id: bytes = peer_id.encode("latin-1")

		self.data_folder: Path = Path(data.get("data_folder", "data"))

		self.active_folder: Path = Path(data.get("active_folder", f"{self.data_folder}/active"))
		self.watch_folder: Path = Path(data.get("watch_folder", f"{self.data_folder}/watch"))
		self.download_folder: Path = Path(data.get("download_folder", f"{self.data_folder}/download"))

		self.use_log_file: bool = data.get("use_log_file", True)
		self.log_path: str = data.get("log_path", f"{self.data_folder}/torrent.log")

		self.disabled_plugins: set[str] = set(data.get("disabled_plugins", []))

		self.port: int = int(data.get("port", 6889))

		self.max_connections = int(data.get("max_connections", 30))

		self.download_peers_limit: int = int(data.get("download_peers_limit", 8))
		self.upload_peers_limit: int = int(data.get("upload_peers_limit", 4))

		self.peer_idle_timeout: float = float(data.get("peer_idle_timeout", 30))
		self.upload_retry_cooldown: float = float(data.get("upload_retry_cooldown", 300))
		self.block_request_timeout: float = float(data.get("block_request_timeout", 60))

		self.peers_file: str = data.get("peers_file", f"{self.data_folder}/peers.dat")

		self.max_cached_pieces: int = int(data.get("max_cached_pieces", 100))
		self.piece_cache_ttl: float = float(data.get("piece_cache_ttl", 15))

		self.dht_port: int = int(data.get("dht_port", 6999))
		self.dht_peers_per_lookup: int = int(data.get("dht_peers_per_lookup", 20))
		self.dht_enabled: bool = _as_bool(data.get("dht_enabled", True))

		# stored, not enforced (see the enforced=False entries in SETTINGS)
		self.incomplete_folder: Path = Path(data.get("incomplete_folder", f"{self.data_folder}/incomplete"))
		self.incomplete_folder_enabled: bool = _as_bool(data.get("incomplete_folder_enabled", False))

		# speed limits in KB/s; 0 means no limit, no separate on/off flag
		self.speed_limit_down: int = int(data.get("speed_limit_down", 0))
		self.speed_limit_up: int = int(data.get("speed_limit_up", 0))

		self.seed_ratio_limit: float = float(data.get("seed_ratio_limit", 2.0))
		self.seed_ratio_limited: bool = _as_bool(data.get("seed_ratio_limited", False))

		self.download_queue_enabled: bool = _as_bool(data.get("download_queue_enabled", False))
		self.download_queue_size: int = int(data.get("download_queue_size", 0))
		self.seed_queue_enabled: bool = _as_bool(data.get("seed_queue_enabled", False))
		self.seed_queue_size: int = int(data.get("seed_queue_size", 0))

		self.peer_limit_per_torrent: int = int(data.get("peer_limit_per_torrent", self.max_connections))

		self.blocklist_enabled: bool = _as_bool(data.get("blocklist_enabled", False))
		self.blocklist_url: str = data.get("blocklist_url", "")

		self.start_added_torrents: bool = _as_bool(data.get("start_added_torrents", True))

		self._data = data

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
		return _SETTINGS_BY_KEY.get(key)

	def get(self, key: str) -> Any:
		setting = _SETTINGS_BY_KEY.get(key)
		if setting is None:
			return None
		return getattr(self, setting.attr, None)

	def set(self, key: str, value: Any) -> SettingStatus:
		setting = _SETTINGS_BY_KEY.get(key)
		if setting is None:
			logger.warning("Ignoring unknown setting '%s'", key)
			return SettingStatus.UNKNOWN

		try:
			cast_value = setting.cast(value)
		except (TypeError, ValueError) as ex:
			logger.warning("Ignoring bad value for '%s': %r (%s)", key, value, ex)
			return SettingStatus.INVALID

		current = getattr(self, setting.attr, None)
		if current == cast_value:
			return SettingStatus.UNCHANGED

		self._data[key] = value if not isinstance(cast_value, Path) else str(cast_value)
		self._save()

		if setting.restart_required:
			logger.warning("Setting '%s' is stored but only takes effect after a restart", key)
			return SettingStatus.RESTART_REQUIRED

		setattr(self, setting.attr, cast_value)
		if not setting.enforced:
			logger.warning("Setting '%s' is stored but NOT enforced: %s", key, setting.note)
		return SettingStatus.CHANGED
