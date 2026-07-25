import logging
import os
from pathlib import Path
from typing import List, Tuple

from yap_torrent.env import Env
from yap_torrent.protocol import load_torrent_file
from yap_torrent.system import System

logger = logging.getLogger(__name__)


class WatcherSystem(System):
	def __init__(self, env: Env):
		super().__init__(env)
		self.last_update = 0

		self.watch_path = Path(env.config.watch_folder)

	async def start(self):
		self.watch_path.mkdir(parents=True, exist_ok=True)

	async def _update(self, delta_time: float):
		files_to_move = await self._load_from_path(self.watch_path)

		# move file to the trash folder
		for file_path, file_name in files_to_move:
			file_path.rename(file_path.parent.joinpath(file_name + ".added"))

	async def _load_from_path(self, path: Path):
		files_list:List[Tuple[Path, str]] = []
		for root, dirs, files in os.walk(path):
			for file_name in files:
				file_path = Path(root).joinpath(file_name)
				if file_path.suffix != ".torrent":
					continue

				files_list.append((file_path, file_name))
				torrent_file_data = load_torrent_file(file_path)

				if not torrent_file_data:
					logger.info(f"Torrent file {file_path} is invalid")
					continue

				self.env.event_bus.dispatch("request.metainfo.add", torrent_file_data)

		return files_list
