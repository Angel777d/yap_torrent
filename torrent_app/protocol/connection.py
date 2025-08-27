# this spec used: https://wiki.theory.org/BitTorrentSpecification

import asyncio
import logging
import struct
import time
from asyncio import StreamReader, StreamWriter, IncompleteReadError
from enum import Enum
from typing import Tuple, Optional

from . import decode
from .structures import PeerInfo

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
	PORT = 9  # <len=0003><id=9><port>
	EXTENDED = 20  # <len=0001+X><id=20><extended message ID>

	ERROR = -2


class Message:
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
	def index(self) -> int:
		if self.__message_id == MessageId.HAVE:
			return struct.unpack("!I", self.__payload)[0]
		raise RuntimeError("wrong message type for index property")

	@property
	def bitfield(self) -> bytes:
		if self.__message_id == MessageId.BITFIELD:
			return self.__payload
		raise RuntimeError("wrong message type for bitfield property")

	@property
	def piece(self) -> Tuple[int, int, bytes]:
		if self.__message_id == MessageId.PIECE:
			return struct.unpack(f"!II{len(self.__payload) - 8}s", self.__payload)
		raise RuntimeError("wrong message type for piece property")

	@property
	def request(self) -> Tuple[int, int, int]:
		if self.__message_id == MessageId.REQUEST:
			return struct.unpack(f"!III", self.__payload)
		raise RuntimeError("wrong message type for request property")

	@property
	def port(self) -> int:
		if self.__message_id == MessageId.PORT:
			return struct.unpack(f"!H", self.__payload)[0]
		raise RuntimeError("wrong message type for 'port' property")

	@property
	def extended(self) -> tuple[int, dict]:
		if self.__message_id == MessageId.EXTENDED:
			return self.__payload[0], decode(self.__payload[1:])
		raise RuntimeError("wrong message type for 'extended' property")

	@property
	def raw_payload(self) -> bytes:
		return self.__payload

	def __repr__(self):
		return self.__str__()

	def __str__(self):
		return self.__message_id.name


def __create_handshake_message(info_hash: bytes, peer_id: bytes, reserved=bytes(8)):
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
	return struct.pack(f"!B{pstrlen}s8s20s20s", pstrlen, PSTR_V1, reserved, info_hash, peer_id)


async def __read_handshake_message(reader: StreamReader) -> Tuple[bytes, bytes, bytes, bytes, bytes]:
	pstrlen = await reader.readexactly(1)
	pstr = await reader.readexactly(int.from_bytes(pstrlen))
	reserved = await reader.readexactly(8)
	info_hash = await reader.readexactly(20)
	peer_id = await reader.readexactly(20)
	return pstrlen, pstr, reserved, info_hash, peer_id


async def connect(peer_info: PeerInfo, info_hash: bytes, local_peer_id: bytes, timeout: float = 1.0,
                  reserved: bytes = bytes(8), local_addr: Optional[Tuple[str, int]] = None,
                  # ('127.0.0.1', 9999)
                  ) -> Optional[Tuple[bytes, StreamReader, StreamWriter, bytes]]:
	logger.debug(f"try connect to {peer_info}")
	assert len(reserved) == 8
	assert len(info_hash) == 20
	try:
		async with asyncio.timeout(timeout):
			reader, writer = await asyncio.open_connection(peer_info.host, peer_info.port, local_addr=local_addr)
	except TimeoutError:
		logger.debug(f"Connection to {peer_info} Failed")
		return None

	message = __create_handshake_message(info_hash, local_peer_id, reserved)
	logger.debug(f"Send handshake to: {peer_info}, message: {message}")

	writer.write(message)
	await writer.drain()
	try:
		async with asyncio.timeout(timeout):
			handshake_response = await __read_handshake_message(reader)
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
	return remote_peer_id, reader, writer, reserved


async def on_connect(
		local_peer_id: bytes,
		reader: StreamReader,
		writer: StreamWriter,
		reserved: bytes = bytes(8),
		timeout: float = 1.0,
):
	try:
		async with asyncio.timeout(timeout):
			pstrlen, pstr, remote_reserved, info_hash, remote_peer_id = await __read_handshake_message(reader)
	except TimeoutError:
		logger.debug(f"Incoming handshake timeout error")
		writer.close()
		return None
	except Exception as ex:
		logger.error(f"Incoming handshake unexpected error {ex}")
		writer.close()
		return None

	try:
		message = __create_handshake_message(info_hash, local_peer_id, reserved)
		logger.debug(f"Send handshake back to: {remote_peer_id}, message: {message}")
		writer.write(message)
		await writer.drain()
	except Exception as ex:
		logger.error(f"Handshake to {remote_peer_id} failed by {ex}")
		writer.close()
		return None

	return pstrlen, pstr, remote_reserved, info_hash, remote_peer_id


class Connection:

	def __init__(self, remote_peer_id: bytes, reader: StreamReader, writer: StreamWriter, timeout: int = 60 * 5):
		self.timeout = timeout

		self.remote_peer_id = remote_peer_id

		self.connection_time = time.monotonic()
		self.last_message_time = time.monotonic()
		self.last_out_time = time.monotonic()

		self.reader: StreamReader = reader
		self.writer: StreamWriter = writer

	def is_dead(self) -> bool:
		return time.monotonic() - self.last_message_time > self.timeout

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
			length = struct.unpack("!I", buffer)[0]

			if length:
				buffer = await self.reader.readexactly(length)
				message = Message.from_bytes(buffer)
			else:
				message = Message(MessageId.KEEP_ALIVE)

		except IncompleteReadError as ex:
			message = Message(MessageId.ERROR)
			logger.debug(f"IncompleteReadError on {self.remote_peer_id}. Exception {ex}")
		except ConnectionResetError as ex:
			message = Message(MessageId.ERROR)
			logger.debug(f"ConnectionResetError on {self.remote_peer_id}. Exception {ex}")

		logger.debug(f"got message {message} from {self.remote_peer_id}")
		self.last_message_time = time.monotonic()

		return message

	async def keep_alive(self) -> None:
		if time.monotonic() - self.last_out_time < 10:
			return
		await self.__send(bytes())

	async def choke(self) -> None:
		await self.__send(struct.pack('!B', MessageId.CHOKE.value))

	async def unchoke(self) -> None:
		await self.__send(struct.pack('!B', MessageId.UNCHOKE.value))

	async def interested(self) -> None:
		await self.__send(struct.pack('!B', MessageId.INTERESTED.value))

	async def not_interested(self) -> None:
		await self.__send(struct.pack('!B', MessageId.NOT_INTERESTED.value))

	async def have(self, piece_index) -> None:
		await self.__send(struct.pack('!BI', MessageId.HAVE.value, piece_index))

	async def bitfield(self, bitfield) -> None:
		await self.__send(struct.pack(f'!B{len(bitfield)}s', MessageId.BITFIELD.value, bitfield))

	async def request(self, piece_index, begin, length) -> None:
		await self.__send(struct.pack('!BIII', MessageId.REQUEST.value, piece_index, begin, length))

	async def piece(self, piece_index: int, begin: int, block: bytes) -> None:
		await self.__send(struct.pack(f'!BII{len(block)}s', MessageId.PIECE.value, piece_index, begin, block))

	async def cancel(self, piece_index, begin, length) -> None:
		await self.__send(struct.pack('!BIII', MessageId.CANCEL.value, piece_index, begin, length))

	async def port(self, port: int) -> None:
		await self.__send(struct.pack('!BH', MessageId.PORT.value, port))

	async def extended(self, ext_id: int, payload: bytes) -> None:
		await self.__send(struct.pack(f'!BB{len(payload)}s', MessageId.EXTENDED.value, ext_id, payload))

	async def __send(self, message: bytes) -> None:
		logger.debug(f"send {Message.from_bytes(message)} message to {self.remote_peer_id}")
		try:
			self.last_out_time = time.monotonic()
			self.writer.write(struct.pack("!I", len(message)))
			self.writer.write(message)
			await self.writer.drain()
		except Exception as ex:
			logger.warning(f"got send error on {self.remote_peer_id}: {ex}")
