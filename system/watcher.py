import os
from pathlib import Path
from shutil import move

from system import System, Config, Storage
from torrent.tracker import load_torrent_file


class Watcher(System):
    def __init__(self, config: Config, storage: Storage):
        super().__init__(config, storage)
        self.last_update = 0

        self.trash_path = Path(config.trash_folder)
        self.watch_path = Path(config.watch_folder)
        self.active_path = Path(config.active_folder)

    async def __check_folders(self):
        if not self.trash_path.exists():
            self.trash_path.mkdir()
        if not self.watch_path.exists():
            self.watch_path.mkdir()
        if not self.active_path.exists():
            self.active_path.mkdir()

    async def start(self) -> System:
        await self.__check_folders()
        await self._load_from_path(self.active_path)
        return self

    async def update(self, delta_time: float):
        files_to_move = await self._load_from_path(self.watch_path)

        # move file to active folder
        for file_path, file_name in files_to_move:
            move(file_path, self.active_path.joinpath(file_name))

    async def _load_from_path(self, path: Path):
        files_list = []
        for root, dirs, files in os.walk(path):
            for file_name in files:
                file_path = Path(root).joinpath(file_name)
                if file_path.suffix != ".torrent":
                    continue

                await self._add_torrent(file_path)
                files_list.append((file_path, file_name))
        return files_list

    async def _add_torrent(self, file_path: Path) -> bool:
        torrent_info = load_torrent_file(file_path)

        if not torrent_info.is_valid():
            print("can't read torrent file:", file_path)
            return False

        print("new torrent added from path:", file_path)
        self.storage.new_torrents[torrent_info.info_hash] = torrent_info
        return True
