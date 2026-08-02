"""What one running RPC server is, and how a client names torrents to it.

This lives apart from `methods` and `mapping` because both of them need it and they
already depend on each other: `methods` imports `build_torrent`, and `build_torrent`
takes the ServerInfo. Defining it in either one makes that a cycle.
"""
import logging
from typing import Any, Dict, Optional

from yap_torrent.components.torrent_ec import TorrentEC
from yap_torrent.env import Env

logger = logging.getLogger(__name__)

_TorrentID = int | str
_TorrentIDs = _TorrentID | list[_TorrentID] | None

DEFAULT_PORT = 9091
DEFAULT_PATH = "/transmission/rpc"
# Transmission serves its own UI next to the RPC on the same port, so a browser pointed
# at the host gets the client and a remote gets the API without a second server.
DEFAULT_WEB_PATH = "/transmission/web"


class TorrentIDs:
	def __init__(self, source: _TorrentIDs = None):
		self.indexes: set[int] = set()
		self.hashes: set[str] = set()
		self.add(source)

	def add(self, ids: _TorrentIDs):
		if ids is None:
			return
		# bool is a subclass of int; reject it before the int branch so a JSON
		# true/false is not silently treated as index 1/0.
		if isinstance(ids, bool):
			logger.warning("Ignoring boolean torrent id: %s", ids)
		elif isinstance(ids, int):
			self.indexes.add(ids)
		elif isinstance(ids, str):
			self.hashes.add(ids.lower())
		elif isinstance(ids, list):
			for _id in ids:
				self.add(_id)
		else:
			logger.warning("Invalid ids argument: %s", ids)

	def contains(self, torrent: TorrentEC, index: int):
		return index in self.indexes or torrent.info_hash.hex() in self.hashes

	def empty(self):
		return not (self.indexes or self.hashes)

	@staticmethod
	def is_recent(ids):
		return ids in ("recently-active", "recently_active")

	@staticmethod
	def read_ids(arguments, recent: "TorrentIDs") -> "TorrentIDs":
		ids = arguments.get("ids")
		if TorrentIDs.is_recent(ids):
			return recent

		return TorrentIDs(ids)

	@property
	def ids(self) -> _TorrentIDs:
		return list(self.indexes) + list(self.hashes)

	def remove(self, other: "TorrentIDs"):
		self.indexes.difference_update(other.indexes)
		self.hashes.difference_update(other.hashes)

	def clear(self):
		self.indexes.clear()
		self.hashes.clear()


class ServerInfo:
	"""What one running RPC server is: its identity, its config block, and the session
	state the methods hand back.

	`name` is the entry point name discovery gave the plugin. It is both the config
	section this server reads and the key its per-torrent state lives under in a
	torrent's custom_data, so it is carried here rather than in a module global — every
	handler already receives this object, and a global would make a second server in one
	process silently share the first one's name.
	"""

	def __init__(self, name: str, env: Env, session_id: str, start_time: float):
		self.name: str = name
		self.env: Env = env
		self.session_id: str = session_id
		self.start_time: float = start_time
		self.recent: TorrentIDs = TorrentIDs()
		self.removed: TorrentIDs = TorrentIDs()

		self.config: Dict[str, Any] = env.config.get_plugin_config(name)
		self.host: str = self.config.get("host", "0.0.0.0")
		self.port: int = int(self.config.get("port", DEFAULT_PORT))
		self.path: str = self.config.get("path", DEFAULT_PATH)

		# the bundled browser UI. It is only ever a client of the RPC below — it holds no
		# state and reaches core through the same methods a Transmission remote uses.
		self.web_enabled: bool = bool(self.config.get("web_enabled", True))
		self.web_path: str = str(self.config.get("web_path", DEFAULT_WEB_PATH)).rstrip("/")

		# Reserved for a future HTTP Basic auth implementation (see RpcServer._check_auth).
		self.auth_username: Optional[str] = self.config.get("username")
		self.auth_password: Optional[str] = self.config.get("password")
