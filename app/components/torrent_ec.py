from typing import Hashable

from core.DataStorage import EntityComponent
from torrent import TorrentInfo


class TorrentInfoEC(EntityComponent):
    def __init__(self, torrent_info: TorrentInfo):
        super().__init__()
        self.info: TorrentInfo = torrent_info

    @classmethod
    def is_hashable(cls) -> bool:
        return True

    def get_hash(self) -> Hashable:
        return self.info.info_hash

# def __choose_rarest(pieces: Tuple[PieceToPeers, ...]):
#     result: List[PieceToPeers] = []
#     max_count = 0
#     for p in pieces:
#         if p.peers_num > max_count:
#             max_count = p.peers_num
#             result = [p]
#         elif p.peers_num == max_count:
#             result.append(p)
#     return random.choice(result)
#     pass
