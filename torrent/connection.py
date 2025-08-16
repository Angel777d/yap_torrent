# this spec used: https://wiki.theory.org/BitTorrentSpecification

import asyncio
import struct
import time
from asyncio import StreamReader, StreamWriter
from enum import Enum, unique, auto
from typing import Tuple

from torrent.structures import PeerInfo


class MessageId(Enum):
	KEEP_ALIVE = -1  # <len=0000>
	CHOKE = 0  # <len=0001><id=0>
	UNCHOKE = 1  # <len=0001><id=1>
	INTERESTED = 2  # <len=0001><id=2>
	NOT_INTERESTED = 3  # <len=0001><id=3>
	HAVE = 4  # <len=0005><id=4><piece index>
	BITFIELD = 5  # <len=0001+X><id=5><bitfield>
	REQUEST = 6  # <len=0013><id=6><index><begin><length>
	PIECE = 7  # <len=0009+X><id=7><index><begin><block>
	CANCEL = 8  # <len=0013><id=8><index><begin><length>

	ERROR = -2


class Message:
	HANDSHAKE_FORMAT = '!B19s8s20s20s'

	def __init__(self):
		self.__message_id: MessageId = MessageId.KEEP_ALIVE
		self.__payload: bytes = bytes()

	@classmethod
	def from_bytes(cls, buffer: bytes):
		message = cls()
		try:
			message.__message_id = MessageId(buffer[0])
		except ValueError:
			message.__message_id = MessageId.ERROR
		message.__payload = buffer[1:]
		return message

	@property
	def message_id(self) -> MessageId:
		return self.__message_id

	@property
	def payload(self):
		if self.__message_id == MessageId.CHOKE:
			return True
		elif self.__message_id == MessageId.UNCHOKE:
			return False
		elif self.__message_id == MessageId.INTERESTED:
			return True
		elif self.__message_id == MessageId.NOT_INTERESTED:
			return False
		elif self.__message_id == MessageId.HAVE:
			return int.from_bytes(self.__payload)
		elif self.__message_id == MessageId.BITFIELD:
			return self.__payload
		elif self.__message_id == MessageId.REQUEST:
			index = int.from_bytes(self.__payload[:4])
			begin = int.from_bytes(self.__payload[4:8])
			length = int.from_bytes(self.__payload[8:])
			return index, begin, length
		elif self.__message_id == MessageId.PIECE:
			index = int.from_bytes(self.__payload[:4])
			begin = int.from_bytes(self.__payload[4:8])
			block = self.__payload[8:]
			return index, begin, block
		elif self.__message_id == MessageId.CANCEL:
			index = int.from_bytes(self.__payload[:4])
			begin = int.from_bytes(self.__payload[4:8])
			length = int.from_bytes(self.__payload[8:])
			return index, begin, length
		else:
			print("unknown message_id")
			return None

	@property
	def index(self) -> int:
		if self.__message_id == MessageId.HAVE:
			return int.from_bytes(self.__payload)
		raise RuntimeError("wrong message type for index property")

	@property
	def bitfield(self) -> bytes:
		if self.__message_id == MessageId.BITFIELD:
			return self.__payload
		raise RuntimeError("wrong message type for bitfield property")

	@property
	def piece(self) -> Tuple[int, int, bytes]:
		if self.__message_id == MessageId.PIECE:
			index = int.from_bytes(self.__payload[:4])
			begin = int.from_bytes(self.__payload[4:8])
			block = self.__payload[8:]
			return index, begin, block
		raise RuntimeError("wrong message type for piece property")

	@property
	def request(self) -> Tuple[int, int, int]:
		if self.__message_id == MessageId.REQUEST:
			index = int.from_bytes(self.__payload[:4])
			begin = int.from_bytes(self.__payload[4:8])
			length = int.from_bytes(self.__payload[8:])
			return index, begin, length
		raise RuntimeError("wrong message type for request property")

	def __repr__(self):
		return self.__str__()

	def __str__(self):
		return self.__message_id.name

	@classmethod
	def create(cls, message_id: MessageId, payload: tuple = None) -> bytes:
		if message_id in (MessageId.CHOKE, MessageId.UNCHOKE, MessageId.INTERESTED,
		                  MessageId.NOT_INTERESTED):  # <len=0005><id=4><piece index>
			message_length = 1
			format_message = '!IB'
			return struct.pack(format_message, message_length, message_id.value)
		elif message_id == MessageId.HAVE:  # <len=0005><id=4><piece index>
			message_length = 5
			format_message = '!IBI'
			return struct.pack(format_message, message_length, message_id.value, payload[0])
		elif message_id == MessageId.BITFIELD:  # <len=0001+X><id=5><bitfield>
			bitfield = payload[0]
			message_length = 1 + len(bitfield)
			format_message = '!IB'
			return struct.pack(format_message, message_length, message_id.value) + bitfield
		elif message_id == MessageId.REQUEST:  # <len=0013><id=6><index><begin><length>
			index, begin, length = payload
			message_length = 13
			format_message = '!IBIII'
			return struct.pack(format_message, message_length, message_id.value, index, begin, length)
		elif message_id == MessageId.PIECE:  # <len=0009+X><id=7><index><begin><block>
			index, begin, block = payload
			message_length = 9 + len(block)
			format_message = '!IBII'
			return struct.pack(format_message, message_length, message_id.value, index, begin) + block
		elif message_id == MessageId.CANCEL:  # <len=0013><id=8><index><begin><length>
			index, begin, length = payload
			message_length = 13
			format_message = '!IBIII'
			return struct.pack(format_message, message_length, message_id.value, index, begin, length)
		else:
			return bytes()

	@classmethod
	def create_handshake_message(cls, info_hash: bytes, peer_id: bytes):
		# Handshake
		# The handshake is a required message and must be the first message transmitted by the client. It is (49+len(pstr)) bytes long.
		#
		# handshake: <pstrlen><pstr><reserved><info_hash><peer_id>
		#
		# pstrlen: string length of <pstr>, as a single raw byte
		# pstr: string identifier of the protocol
		# reserved: eight (8) reserved bytes. All current implementations use all zeroes. Each bit in these bytes can be used to change the behavior of the protocol. An email from Bram suggests that trailing bits should be used first, so that leading bits may be used to change the meaning of trailing bits.
		# info_hash: 20-byte SHA1 hash of the info key in the metainfo file. This is the same info_hash that is transmitted in tracker requests.
		# peer_id: 20-byte string used as a unique ID for the client. This is usually the same peer_id that is transmitted in tracker requests (but not always e.g. an anonymity option in Azureus).
		# In version 1.0 of the BitTorrent protocol, pstrlen = 19, and pstr = "BitTorrent protocol".

		pstr = b'BitTorrent protocol'
		pstrlen = len(pstr)
		reserved = bytearray(8)
		return struct.pack(cls.HANDSHAKE_FORMAT, pstrlen, pstr, reserved, info_hash, peer_id)

	@classmethod
	def parse_handshake_message(cls, buffer: bytes):
		return struct.unpack_from(cls.HANDSHAKE_FORMAT, buffer)


@unique
class ConnectionState(Enum):
	Created = auto()
	Handshake = auto()
	Connected = auto()
	Disconnected = auto()


class Connection:

	def __init__(self, timeout: int = 60 * 3):
		self.timeout = timeout

		self.remote_peer_id = None

		self.connection_time = time.time()
		self.last_message_time = time.time()

		self.reader: StreamReader = None
		self.writer: StreamWriter = None

		self.state = ConnectionState.Created

	def connect(self, peer_info: PeerInfo, info_hash: bytes, peer_id: bytes, bitfield: bytes) -> asyncio.Task:
		self.state = ConnectionState.Handshake
		return asyncio.create_task(self._connect(peer_info, info_hash, peer_id, bitfield))

	async def _connect(self, peer_info: PeerInfo, info_hash: bytes, peer_id: bytes, bitfield: bytes) -> None:
		self.connection_time = time.time()

		self.reader, self.writer = await asyncio.open_connection(peer_info.host, peer_info.port)

		message = Message.create_handshake_message(info_hash, peer_id)
		print(f"Send handshake to: {peer_info}, message: {message}")
		self.writer.write(message)
		handshake_response = await self.reader.readexactly(len(message))
		_, _, _, remote_info_hash, remote_peer_id = Message.parse_handshake_message(handshake_response)
		print(f"Received handshake from: {remote_peer_id} {peer_info}, message: {handshake_response}")

		self.remote_peer_id = remote_peer_id
		self.last_message_time = time.time()

		# send bitfield just after the handshake if any
		if bitfield:
			self.bitfield(bitfield)

		if remote_info_hash == info_hash:
			self.state = ConnectionState.Connected
		else:
			self.state = ConnectionState.Disconnected

	def is_dead(self) -> bool:
		return self.state == ConnectionState.Disconnected or (time.time() - self.last_message_time > self.timeout)

	def close(self) -> None:
		self.last_message_time = .0
		# in case connection was not created at all
		if self.writer:
			self.writer.close()
		self.writer = None
		self.reader = None

	async def read(self) -> Message:
		length = await self.reader.read(4)
		length = int.from_bytes(length)
		if length == 0:
			return self.__on_message(Message())

		buffer = bytes()
		while len(buffer) < length:
			buffer += await self.reader.read(length)

		message = Message.from_bytes(buffer)
		print("got message:", message)

		return self.__on_message(message)

	def choke(self) -> None:
		self.__send(Message.create(MessageId.CHOKE))

	def unchoke(self) -> None:
		self.__send(Message.create(MessageId.UNCHOKE))

	def interested(self) -> None:
		self.__send(Message.create(MessageId.INTERESTED))

	def not_interested(self) -> None:
		self.__send(Message.create(MessageId.NOT_INTERESTED))

	def have(self, piece_index) -> None:
		self.__send(Message.create(MessageId.HAVE, (piece_index,)))

	def bitfield(self, bitfield) -> None:
		self.__send(Message.create(MessageId.BITFIELD, (bitfield,)))

	def request(self, piece_index, begin, length) -> None:
		self.__send(Message.create(MessageId.REQUEST, (piece_index, begin, length)))

	def piece(self, piece_index, begin, block) -> None:
		self.__send(Message.create(MessageId.PIECE, (piece_index, begin, block)))

	def cancel(self, piece_index, begin, length) -> None:
		self.__send(Message.create(MessageId.CANCEL, (piece_index, begin, length)))

	def __send(self, message: bytes) -> None:
		print("send message:", Message.from_bytes(message[4:]))
		self.writer.write(message)

	def __on_message(self, message: Message):
		self.last_message_time = time.time()
		return message
