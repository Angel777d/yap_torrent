import json
import logging
from typing import Dict, Any

logger = logging.getLogger(__name__)


class Config:
	DEFAULT_CONFIG = "config.json"

	def __init__(self, path=DEFAULT_CONFIG):
		data: Dict[str, Any] = {}
		try:
			with open(path, "r") as f:
				data = json.load(f)
		except FileNotFoundError:
			logger.warning(f"Config file not found at {path}. Using default settings.")
		except json.JSONDecodeError:
			logger.warning(f"Config file at {path} is invalid. Using default settings.")

		self.data_folder = data.get("data_folder", "data")

		self.active_folder = data.get("active_folder", f"{self.data_folder}/active")
		self.watch_folder = data.get("watch_folder", f"{self.data_folder}/watch")
		self.download_folder = data.get("download_folder", f"{self.data_folder}/download")
		self.trash_folder = data.get("trash_folder", f"{self.data_folder}/trash")

		self.disabled_plugins: set[str] = set(data.get("disabled_plugins", []))

		self.port: int = int(data.get("port", 6889))

		self.max_connections = int(data.get("max_connections", 30))

		self.dht_port: int = int(data.get("dht_port", 6999))
		self._data = data

	@property
	def data(self) -> Dict[str, Any]:
		return self._data
