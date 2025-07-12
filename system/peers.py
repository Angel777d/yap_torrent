import asyncio
from asyncio import Task
from typing import Tuple, List

from system import System, Config, Storage, ActiveTorrent, PieceData
from torrent.connection import Connection, MessageId
from torrent.structures import BitField


class PeerConnection:
    def __init__(self, connection: Connection, active_torrent: ActiveTorrent, storage: Storage):
        self.connection = connection
        self.active_torrent = active_torrent
        self.storage = storage
        self.task = asyncio.create_task(self.load_torrent())

    def close(self):
        self.task.cancel()
        self.connection.close()

    async def load_torrent(self):
        remote_bitfield = BitField(self.active_torrent.info.pieces.num)
        self.connection.interested()
        self.connection.unchoke()

        piece = None

        while True:
            message = await self.connection.read()
            if message.message_id == MessageId.BITFIELD:
                remote_bitfield.update(message.payload)
            if message.message_id == MessageId.HAVE:
                print(f"remote have piece {message.payload}")
                remote_bitfield.set_at(message.payload)
            if message.message_id == MessageId.UNCHOKE:
                pass
            if message.message_id == MessageId.PIECE:
                index, begin, block = message.payload
                piece.append(begin, block)

                if piece.completed:
                    print(f"piece {piece.index} completed")
                    self.connection.have(piece.index)
                    self.storage.loaded_pieces.setdefault(self.active_torrent.info.info_hash, []).append(piece)
                    piece = None
                else:
                    # continue download
                    self.connection.request(piece.index, piece.get_next_begin(), piece.block_size)

            if not piece:
                index = remote_bitfield.get_next_index(self.active_torrent.bitfield)
                if index > -1:
                    print(f"piece {index} download started")
                    piece = PieceData(index, self.active_torrent.info.pieces.piece_length)
                    self.connection.request(piece.index, piece.get_next_begin(), piece.block_size)


class Peers(System):

    def __init__(self, config: Config, storage: Storage):
        super().__init__(config, storage)
        self.connections: List[PeerConnection] = []

    async def update(self, delta_time: float):
        # check connections
        connections_to_remove = []
        for peer_conn in self.connections:
            if peer_conn.task.done() or not peer_conn.connection.is_alive():
                connections_to_remove.append(peer_conn)

        # remove outdated connections
        for peer_conn in connections_to_remove:
            peer_conn.close()
            self.connections.remove(peer_conn)

        peer_id = self.storage.peer_id
        for info_hash, peers in self.storage.peers.items():

            active_torrent = self.storage.active_torrents.get(info_hash, None)
            if not active_torrent:
                print(f"torrent hash {info_hash} not found")
                continue

            for peer in peers:

                if len(self.connections) > self.config.max_connections:
                    break

                connection = Connection()
                connection_good = await connection.connect(peer, info_hash, peer_id)
                if not connection_good:
                    continue

                self.connections.append(PeerConnection(connection, active_torrent, self.storage))
