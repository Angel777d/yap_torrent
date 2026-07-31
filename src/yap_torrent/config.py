import json
import logging
import random
import string
from pathlib import Path
from typing import Any, Dict

logger = logging.getLogger(__name__)

_PEER_ID_PREFIX = "-PY0001-"


def generate_peer_id() -> str:
	suffix = "".join(random.choices(string.ascii_letters + string.digits, k=20 - len(_PEER_ID_PREFIX)))
	return _PEER_ID_PREFIX + suffix


def as_bool(value: Any) -> bool:
	"""Read a JSON-ish truth value; "false" is false, unlike bool("false")."""
	if isinstance(value, str):
		return value.strip().lower() in ("1", "true", "yes", "on")
	return bool(value)


# Properties read once, while something is being set up — the listening sockets and whether
# the DHT runs at all. Writing one stores it for the next run and leaves the running client
# on the old value, so what core reports is always what core is actually doing.
# TODO: restart the affected part instead, and this set goes away (see tasks/backlog.md).
STARTUP_ONLY = frozenset({"port", "dht_port", "dht_enabled"})


class Config:
	"""Core's settings: plain properties with defaults, overridden from `config.json`.

	Nothing here knows what a user interface calls a property, whether it is worth
	offering, or what happens when it changes — that is a *setting*, and it belongs to the
	plugin that offers it (see `yap_torrent/settings.py`).
	"""

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

		self.data_folder: Path = Path(data.get("data_folder", "data"))

		self.active_folder: Path = Path(data.get("active_folder", f"{self.data_folder}/active"))
		self.watch_folder: Path = Path(data.get("watch_folder", f"{self.data_folder}/watch"))
		self.download_folder: Path = Path(data.get("download_folder", f"{self.data_folder}/download"))

		self.use_log_file: bool = as_bool(data.get("use_log_file", True))
		self.log_path: str = str(data.get("log_path", f"{self.data_folder}/torrent.log"))

		self.disabled_plugins: set[str] = set(data.get("disabled_plugins", []))

		self.port: int = int(data.get("port", 6889))

		self.max_connections: int = int(data.get("max_connections", 30))

		self.download_peers_limit: int = int(data.get("download_peers_limit", 8))
		self.upload_peers_limit: int = int(data.get("upload_peers_limit", 4))

		self.peer_idle_timeout: float = float(data.get("peer_idle_timeout", 30))
		self.upload_retry_cooldown: float = float(data.get("upload_retry_cooldown", 300))
		self.block_request_timeout: float = float(data.get("block_request_timeout", 60))

		self.peers_file: str = str(data.get("peers_file", f"{self.data_folder}/peers.dat"))

		self.max_cached_pieces: int = int(data.get("max_cached_pieces", 100))
		self.piece_cache_ttl: float = float(data.get("piece_cache_ttl", 15))

		self.dht_port: int = int(data.get("dht_port", 6999))
		self.dht_peers_per_lookup: int = int(data.get("dht_peers_per_lookup", 20))
		self.dht_enabled: bool = as_bool(data.get("dht_enabled", True))

		self.incomplete_folder: Path = Path(data.get("incomplete_folder", f"{self.data_folder}/incomplete"))
		self.incomplete_folder_enabled: bool = as_bool(data.get("incomplete_folder_enabled", False))

		# speed limits in KB/s; 0 means no limit, no separate on/off flag
		self.speed_limit_down: int = int(data.get("speed_limit_down", 0))
		self.speed_limit_up: int = int(data.get("speed_limit_up", 0))

		self.seed_ratio_limit: float = float(data.get("seed_ratio_limit", 2.0))
		self.seed_ratio_limited: bool = as_bool(data.get("seed_ratio_limited", False))

		self.download_queue_enabled: bool = as_bool(data.get("download_queue_enabled", False))
		self.download_queue_size: int = int(data.get("download_queue_size", 0))
		self.seed_queue_enabled: bool = as_bool(data.get("seed_queue_enabled", False))
		self.seed_queue_size: int = int(data.get("seed_queue_size", 0))

		self.peer_limit_per_torrent: int = int(data.get("peer_limit_per_torrent", self.max_connections))

		self.blocklist_enabled: bool = as_bool(data.get("blocklist_enabled", False))
		self.blocklist_url: str = str(data.get("blocklist_url", ""))

		self.start_added_torrents: bool = as_bool(data.get("start_added_torrents", True))

		peer_id = data.get("peer_id")
		if not peer_id:
			peer_id = generate_peer_id()
			data["peer_id"] = peer_id
			self.save()
		self.peer_id: bytes = peer_id.encode("latin-1")

	def has(self, key: str) -> bool:
		"""Whether `key` names a config property (and not something private)."""
		return not key.startswith("_") and hasattr(self, key)

	def store(self, key: str, value: Any) -> None:
		"""Write a property to `config.json` without touching the running value."""
		self._data[key] = str(value) if isinstance(value, Path) else value
		self.save()

	def apply(self, key: str, value: Any) -> None:
		"""Change a property here and on disk."""
		setattr(self, key, value)
		self.store(key, value)

	def save(self):
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
		self.save()
