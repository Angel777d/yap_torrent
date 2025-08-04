import asyncio
import math

from app import System, Env
from app.components.bitfield_ec import BitfieldEC
from app.components.peer_ec import PeerPendingEC, PeerInfoEC, PeerConnectionEC, PeerHandshakeEC, PeerActiveEC, \
    PeerUpdateBitfieldEC
from app.components.torrent_ec import TorrentInfoEC
from core.DataStorage import Entity
from torrent.connection import Connection, ConnectionState, MessageId


# async def load_torrent(self, torrent_entity: Entity, connection: Connection):
#     info = torrent_entity.get_component(TorrentInfoEC).info
#     active_ec = torrent_entity.get_component(ActiveTorrentEC)
#     remote_bitfield = BitField(info.pieces.num)
#     connection.interested()
#     connection.unchoke()
#
#     piece = None
#
#     while True:
#         message = await connection.read()
#         if message.message_id == MessageId.BITFIELD:
#             remote_bitfield.update(message.payload)
#         if message.message_id == MessageId.HAVE:
#             print(f"remote have piece {message.payload}")
#             remote_bitfield.set_at(message.payload)
#         if message.message_id == MessageId.UNCHOKE:
#             pass
#         if message.message_id == MessageId.PIECE:
#             index, begin, block = message.payload
#             piece.append(begin, block)
#
#             if piece.completed:
#                 print(f"piece {piece.index} completed")
#                 connection.have(piece.index)
#                 self.storage.loaded_pieces.setdefault(info.info_hash, []).append(piece)
#                 piece = None
#             else:
#                 # continue download
#                 connection.request(piece.index, piece.get_next_begin(), piece.block_size)
#
#         if not piece:
#             index = remote_bitfield.get_next_index(active_ec.bitfield)
#             if index > -1:
#                 print(f"piece {index} download started")
#                 piece = PieceData(index, info.pieces.piece_length)
#                 connection.request(piece.index, piece.get_next_begin(), piece.block_size)


class PeerSystem(System):

    def __init__(self, env: Env):
        super().__init__(env)

    async def update(self, delta_time: float):
        await self.remove_outdated()
        await self.update_peers()
        await self.process_handshake_peers()
        await self.process_active_peers()

    async def remove_outdated(self):
        ds = self.env.data_storage
        peer_connections = ds.get_collection(PeerConnectionEC).entities
        for entity in peer_connections:
            if entity.get_component(PeerConnectionEC).connection.is_dead():
                ds.remove_entity(entity)

    async def update_peers(self):
        ds = self.env.data_storage
        my_peer_id = self.env.peer_id

        active_collection = ds.get_collection(PeerConnectionEC)
        # check capacity
        if len(active_collection) >= self.env.config.max_connections:
            return

        # sort and filter pending peers
        pending_peers = ds.get_collection(PeerPendingEC).entities
        # TODO: implement
        pass

        # connect to new peers
        while len(active_collection) < self.env.config.max_connections and pending_peers:
            peer_entity = pending_peers.pop(0)
            peer_entity.remove_component(PeerPendingEC)

            peer_ec = peer_entity.get_component(PeerInfoEC)

            connection = Connection()

            torrent_entity = ds.get_collection(TorrentInfoEC).find(peer_ec.info_hash)
            torrent_info = torrent_entity.get_component(TorrentInfoEC).info

            peer_entity.add_component(BitfieldEC(BitfieldEC.create_empty(torrent_info)))
            peer_entity.add_component(PeerConnectionEC(connection))
            peer_entity.add_component(PeerHandshakeEC(
                connection.connect(peer_ec.peer_info, peer_ec.info_hash, my_peer_id)
            ))

    async def process_handshake_peers(self):
        ds = self.env.data_storage
        handshake_peers = ds.get_collection(PeerHandshakeEC).entities

        for entity in handshake_peers:
            peer_connection_ec = entity.get_component(PeerConnectionEC)
            connection = peer_connection_ec.connection

            if connection.state == ConnectionState.Created:
                continue

            # handshake in progress. waiting for it
            if connection.state == ConnectionState.Handshake:
                continue

            # we can start do something
            if connection.state == ConnectionState.Connected:
                entity.remove_component(PeerHandshakeEC)
                entity.add_component(PeerActiveEC(asyncio.create_task(self._listen(entity))))

    async def process_active_peers(self):
        ds = self.env.data_storage

        # wait for bitfield or have. mark bitfield update
        # mark peer as have parts
        # send interested
        # wait unchoke
        # choose piece to download
        # ask for block, repeat
        # piece complete. save it, send have to all (potential interested???) related peers
        # no more pieces. send not interested to peer

        # PeerInfoEC - has peer info: address and port
        # PeerPendingEC - tmp. waiting in queue
        # PeerConnectionEC - start connection
        # PeerHandshakeEC - tmp. handshake in progress
        # PeerActiveEC - connection and handshake done
        # PeerUpdateBitfieldEC

        updated_bits = ds.get_collection(PeerUpdateBitfieldEC).entities
        for entity in updated_bits:
            remote = entity.get_component(BitfieldEC)

    async def _listen(self, peer_entity: Entity):
        ds = self.env.data_storage
        peer_ec = peer_entity.get_component(PeerInfoEC)
        peer_connection_ec = peer_entity.get_component(PeerConnectionEC)
        bitfield_ec = peer_entity.get_component(BitfieldEC)

        torrent_entity = ds.get_collection(TorrentInfoEC).find(peer_ec.info_hash)

        connection = peer_connection_ec.connection
        peer_id = connection.remote_peer_id
        torrent_info = torrent_entity.get_component(TorrentInfoEC).info

        # connection.interested()
        # connection.unchoke()

        while True:
            message = await connection.read()

            if message.message_id == MessageId.PIECE:
                index, begin, block = message.piece

                # must be created at first piece request
                # piece_entity = ds.get_collection(PieceEC).find(PieceEC.make_hash(peer_ec.info_hash, index))
                # piece_ec = piece_entity.get_component(PieceEC)
                # piece_ec.data.append(begin, block)
                # if piece_ec.data.completed:
                #     piece_ec.add_marker(CompletedPieceEC)

            elif message.message_id == MessageId.HAVE:
                bitfield_ec.set(message.index)
                bitfield_ec.add_marker(PeerUpdateBitfieldEC)
                pass
            elif message.message_id == MessageId.CHOKE:
                pass
            elif message.message_id == MessageId.UNCHOKE:
                pass
            elif message.message_id == MessageId.INTERESTED:
                pass
            elif message.message_id == MessageId.NOT_INTERESTED:
                pass
            elif message.message_id == MessageId.BITFIELD:
                bitfield_ec.update(message.bitfield)
                bitfield_ec.add_marker(PeerUpdateBitfieldEC)
                # mark peer to update bitfield
                pass
