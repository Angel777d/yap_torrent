import json
import logging
import random
import string
from pathlib import Path
from typing import Dict, Any

logger = logging.getLogger(__name__)

# Azureus-style 20-byte peer id: "-PY0001-" + 12 random chars
_PEER_ID_PREFIX = "-PY0001-"


def generate_peer_id() -> str:
	suffix = "".join(random.choices(string.ascii_letters + string.digits, k=20 - len(_PEER_ID_PREFIX)))
	return _PEER_ID_PREFIX + suffix


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

		# a stable per-instance peer id, generated once and persisted back to the config
		peer_id = data.get("peer_id")
		if not peer_id:
			peer_id = generate_peer_id()
			data["peer_id"] = peer_id
			self._data = data
			# only rewrite an existing config file; never create junk for missing/test paths
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

		# client-wide peer queue limits, and the only cap on established connections.
		# Contested slots go to the peer whose torrent has the lower priority number.
		self.download_peers_limit: int = int(data.get("download_peers_limit", 8))
		self.upload_peers_limit: int = int(data.get("upload_peers_limit", 4))

		# seconds a connection may stay outside both queues before it is dropped
		self.peer_idle_timeout: float = float(data.get("peer_idle_timeout", 30))
		# seconds before redialling a peer that only *might* want our pieces
		self.upload_retry_cooldown: float = float(data.get("upload_retry_cooldown", 300))

		# global known-peers store
		self.peers_file: str = data.get("peers_file", f"{self.data_folder}/peers.dat")

		# completed pieces held in memory, and how long an idle one survives before eviction
		self.max_cached_pieces: int = int(data.get("max_cached_pieces", 100))
		self.piece_cache_ttl: float = float(data.get("piece_cache_ttl", 15))

		self.dht_port: int = int(data.get("dht_port", 6999))
		# peers to collect from a single DHT get_peers walk before stopping
		self.dht_peers_per_lookup: int = int(data.get("dht_peers_per_lookup", 20))
		self._data = data

	def _save(self):
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
