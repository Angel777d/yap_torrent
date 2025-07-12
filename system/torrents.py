import time

from system import System, Config, Storage, ActiveTorrent
from torrent.tracker import make_announce


class Torrents(System):

    def __init__(self, config: Config, storage: Storage):
        super().__init__(config, storage)

    # async def start(self) -> 'System':
    #     return await super().start()

    async def update(self, delta_time: float):
        event = "started"  # "started", "completed", "stopped"
        current_time = time.time()

        for info_hash, torrent_info in self.storage.new_torrents.items():
            if info_hash in self.storage.active_torrents:
                print("torrent already active. skipping", torrent_info.name)
                continue

            self.storage.active_torrents[info_hash] = ActiveTorrent.load(torrent_info)

        for active_torrent in self.storage.active_torrents.values():

            if active_torrent.last_update_time + active_torrent.interval <= current_time:
                await self.__tracker_announce(active_torrent, event)

    async def __tracker_announce(self, active_torrent: ActiveTorrent, event: str = "started"):
        peer_id = self.storage.peer_id
        external_ip = self.storage.external_ip
        port = self.config.port
        info_hash = active_torrent.info.info_hash
        announce = active_torrent.info.announce

        left: int = active_torrent.info.size - active_torrent.downloaded

        print(f"make announce to: {announce}")
        result = make_announce(
            announce,
            info_hash,
            peer_id=peer_id,
            downloaded=active_torrent.downloaded,
            uploaded=active_torrent.uploaded,
            left=left,
            port=port,
            ip=external_ip,
            event=event,
            compact=1,
            tracker_id=active_torrent.tracker_id
        )

        active_torrent.save_announce(result)

        old_peers = self.storage.peers.setdefault(info_hash, set())
        self.storage.peers[info_hash] = old_peers.union(set(result.peers))


