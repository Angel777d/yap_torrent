# this spec used: https://wiki.theory.org/BitTorrentSpecification

import asyncio
import logging
import struct
import time
from asyncio import StreamReader, StreamWriter, IncompleteReadError
from enum import Enum, unique, auto
from typing import Tuple

from torrent.structures import PeerInfo

logger = logging.getLogger(__name__)


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

	def __init__(self, message_id: MessageId = MessageId.KEEP_ALIVE):
		self.__message_id: MessageId = message_id
		self.__payload: bytes = bytes()

	@classmethod
	def from_bytes(cls, buffer: bytes):
		message = cls()

		# KEEP_ALIVE case
		if not buffer:
			return message

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
			logger.warning("unknown message_id")
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
		if message_id == MessageId.KEEP_ALIVE:  # <len=0000>
			return b'0000'
		elif message_id in (MessageId.CHOKE, MessageId.UNCHOKE, MessageId.INTERESTED,
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

	def __init__(self, timeout: int = 30):
		self.timeout = timeout

		self.remote_peer_id = None
		self.host = ""

		self.connection_time = time.time()
		self.last_message_time = time.time()
		self.last_out_time = time.time()

		self.reader: StreamReader = None
		self.writer: StreamWriter = None

		self.state = ConnectionState.Created

	async def connect(self, peer_info: PeerInfo, attempt: int, info_hash: bytes, peer_id: bytes,
	                  bitfield: bytes) -> None:

		self.host = peer_info.host
		self.state = ConnectionState.Handshake
		self.connection_time = time.time()

		logger.debug(f"try connect to {peer_info} attempt {attempt}")

		try:
			async with asyncio.timeout(1):
				self.reader, self.writer = await asyncio.open_connection(peer_info.host, peer_info.port + attempt)
		except TimeoutError:
			self.state = ConnectionState.Disconnected
			logger.debug(f"Connection to {peer_info} attempt {attempt} failed")
			return

		message = Message.create_handshake_message(info_hash, peer_id)
		logger.debug(f"Send handshake to: {peer_info}, message: {message}")

		self.writer.write(message)
		await self.writer.drain()
		try:
			async with asyncio.timeout(1):
				handshake_response = await self.reader.readexactly(len(message))
		except TimeoutError:
			self.state = ConnectionState.Disconnected
			logger.debug(f"Handshake to {peer_info} failed")
			return

		_, _, _, remote_info_hash, remote_peer_id = Message.parse_handshake_message(handshake_response)
		logger.debug(f"Received handshake from: {remote_peer_id} {peer_info}, message: {handshake_response}")

		self.remote_peer_id = remote_peer_id
		self.last_message_time = time.time()

		# send bitfield just after the handshake if any
		if bitfield:
			await self.bitfield(bitfield)

		if remote_info_hash == info_hash:
			self.state = ConnectionState.Connected
			logger.info(f"Connected to peer: {peer_info}. Peer id: {remote_peer_id}")
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
		try:
			buffer = await self.reader.readexactly(4)
			length = int.from_bytes(buffer[:4])

			if length:
				buffer = await self.reader.readexactly(length)
				message = Message.from_bytes(buffer[:length])
			else:
				message = Message(MessageId.KEEP_ALIVE)

		except IncompleteReadError as ex:
			message = Message(MessageId.ERROR)
			logger.info(f"IncompleteReadError on {self.remote_peer_id} {self.host}. Exception {ex}")
		except ConnectionResetError as ex:
			message = Message(MessageId.ERROR)
			logger.info(f"ConnectionResetError on {self.remote_peer_id} {self.host}. Exception {ex}")

		logger.debug(f"got message {message} from {self.host}")
		return self.__on_message(message)

	async def keep_alive(self) -> None:
		if time.time() - self.last_out_time < 10:
			return
		await self.__send(Message.create(MessageId.KEEP_ALIVE))

	async def choke(self) -> None:
		await self.__send(Message.create(MessageId.CHOKE))

	async def unchoke(self) -> None:
		await self.__send(Message.create(MessageId.UNCHOKE))

	async def interested(self) -> None:
		await self.__send(Message.create(MessageId.INTERESTED))

	async def not_interested(self) -> None:
		await self.__send(Message.create(MessageId.NOT_INTERESTED))

	async def have(self, piece_index) -> None:
		await self.__send(Message.create(MessageId.HAVE, (piece_index,)))

	async def bitfield(self, bitfield) -> None:
		await self.__send(Message.create(MessageId.BITFIELD, (bitfield,)))

	async def request(self, piece_index, begin, length) -> None:
		await self.__send(Message.create(MessageId.REQUEST, (piece_index, begin, length)))

	async def piece(self, piece_index, begin, block) -> None:
		await self.__send(Message.create(MessageId.PIECE, (piece_index, begin, block)))

	async def cancel(self, piece_index, begin, length) -> None:
		await self.__send(Message.create(MessageId.CANCEL, (piece_index, begin, length)))

	async def __send(self, message: bytes) -> None:
		logger.debug(f"send {Message.from_bytes(message[4:])} message to {self.remote_peer_id} {self.host}")
		try:
			self.last_out_time = time.time()
			self.writer.write(message)
			await self.writer.drain()
		except Exception as ex:
			logger.warning(f"got send error on {self.remote_peer_id} {self.host}: {ex}")

	def __on_message(self, message: Message):
		self.last_message_time = time.time()
		return message
