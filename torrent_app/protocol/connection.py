# this spec used: https://wiki.theory.org/BitTorrentSpecification

import asyncio
import logging
import struct
import time
from asyncio import StreamReader, StreamWriter, IncompleteReadError
from enum import Enum, unique, auto
from typing import Tuple, Optional

from torrent_app.protocol.structures import PeerInfo

logger = logging.getLogger(__name__)

PSTR_V1 = b'BitTorrent protocol'


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

		pstrlen = len(PSTR_V1)
		reserved = bytearray(8)
		return struct.pack(cls.HANDSHAKE_FORMAT, pstrlen, PSTR_V1, reserved, info_hash, peer_id)

	@classmethod
	def parse_handshake_message(cls, buffer: bytes) -> Tuple[bytes, bytes, bytes, bytes, bytes]:
		return struct.unpack_from(cls.HANDSHAKE_FORMAT, buffer)


@unique
class ConnectionState(Enum):
	Created = auto()
	Handshake = auto()
	Connected = auto()
	Disconnected = auto()


async def read_handshake_message(reader: StreamReader) -> Tuple[bytes, bytes, bytes, bytes, bytes]:
	pstrlen = await reader.readexactly(1)
	pstr = await reader.readexactly(int.from_bytes(pstrlen))
	reserved = await reader.readexactly(8)
	info_hash = await reader.readexactly(20)
	peer_id = await reader.readexactly(20)
	return pstrlen, pstr, reserved, info_hash, peer_id


async def on_connect(local_peer_id: bytes, reader: StreamReader, writer: StreamWriter, timeout: float = 1.0):
	try:
		async with asyncio.timeout(timeout):
			pstrlen, pstr, reserved, info_hash, remote_peer_id = await read_handshake_message(reader)
	except TimeoutError:
		logger.debug(f"Incoming handshake timeout error")
		writer.close()
		return None
	except Exception as ex:
		logger.error(f"Incoming handshake unexpected error {ex}")
		writer.close()
		return None

	try:
		message = Message.create_handshake_message(info_hash, local_peer_id)
		logger.debug(f"Send handshake back to: {remote_peer_id}, message: {message}")
		writer.write(message)
		await writer.drain()
	except Exception as ex:
		logger.error(f"Handshake to {remote_peer_id} failed by {ex}")
		writer.close()
		return None

	return pstrlen, pstr, reserved, info_hash, remote_peer_id


async def connect(peer_info: PeerInfo, info_hash: bytes, local_peer_id: bytes, timeout: float = 1.0) -> Optional[
	Tuple[bytes, StreamReader, StreamWriter]]:
	logger.debug(f"try connect to {peer_info}")

	try:
		async with asyncio.timeout(timeout):
			reader, writer = await asyncio.open_connection(peer_info.host, peer_info.port)
	except TimeoutError:
		logger.debug(f"Connection to {peer_info} Failed")
		return None

	message = Message.create_handshake_message(info_hash, local_peer_id)
	logger.debug(f"Send handshake to: {peer_info}, message: {message}")

	writer.write(message)
	await writer.drain()
	try:
		async with asyncio.timeout(timeout):
			handshake_response = await read_handshake_message(reader)
	except TimeoutError:
		logger.debug(f"Handshake to {peer_info} Failed")
		writer.close()
		return None
	except Exception as ex:
		logger.error(f"Handshake to {peer_info} failed by {ex}")
		writer.close()
		return None

	pstrlen, pstr, reserved, remote_info_hash, remote_peer_id = handshake_response
	logger.debug(f"Received handshake from: {remote_peer_id} {peer_info}, message: {handshake_response}")

	logger.info(f"Connected to peer: {peer_info}. Peer id: {remote_peer_id}")
	return remote_peer_id, reader, writer


class Connection:

	def __init__(self, remote_peer_id: bytes, reader: StreamReader, writer: StreamWriter, timeout: int = 60 * 5):
		self.timeout = timeout

		self.remote_peer_id = remote_peer_id

		self.connection_time = time.time()
		self.last_message_time = time.time()
		self.last_out_time = time.time()

		self.reader: StreamReader = reader
		self.writer: StreamWriter = writer

	def is_dead(self) -> bool:
		return time.time() - self.last_message_time > self.timeout

	def close(self) -> None:
		logger.debug(f"Close connection {self.remote_peer_id}")

		self.last_message_time = .0
		self.remote_peer_id = bytes()
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
			logger.debug(f"IncompleteReadError on {self.remote_peer_id}. Exception {ex}")
		except ConnectionResetError as ex:
			message = Message(MessageId.ERROR)
			logger.debug(f"ConnectionResetError on {self.remote_peer_id}. Exception {ex}")

		logger.debug(f"got message {message} from {self.remote_peer_id}")
		self.last_message_time = time.time()

		return message

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

	async def piece(self, piece_index: int, begin: int, block: bytes) -> None:
		await self.__send(Message.create(MessageId.PIECE, (piece_index, begin, block)))

	async def cancel(self, piece_index, begin, length) -> None:
		await self.__send(Message.create(MessageId.CANCEL, (piece_index, begin, length)))

	async def __send(self, message: bytes) -> None:
		logger.debug(f"send {Message.from_bytes(message[4:])} message to {self.remote_peer_id}")
		try:
			self.last_out_time = time.time()
			self.writer.write(message)
			await self.writer.drain()
		except Exception as ex:
			logger.warning(f"got send error on {self.remote_peer_id}: {ex}")
