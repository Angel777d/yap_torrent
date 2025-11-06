import json
from typing import Dict, Any


class Config:
	DEFAULT_CONFIG = "config.json"

	def __init__(self, path=DEFAULT_CONFIG):
		with open(path, "r") as f:
			data: Dict[str, Any] = json.load(f)
			self.data_folder = data.get("data_folder", "data")

			self.active_folder = data.get("active_folder", f"{self.data_folder}/active")
			self.watch_folder = data.get("watch_folder", f"{self.data_folder}/watch")
			self.download_folder = data.get("download_folder", f"{self.data_folder}/download")
			self.trash_folder = data.get("trash_folder", f"{self.data_folder}/trash")

			self.disabled_plugins: set[str] = set(data.get("disabled_plugins", []))

			self.port: int = int(data.get("port", 6889))

			self.max_connections = int(data.get("max_connections", 15))

			self.dht_port: int = int(data.get("dht_port", 6999))
			self._data = data

	@property
	def data(self) -> Dict[str, Any]:
		return self._data
