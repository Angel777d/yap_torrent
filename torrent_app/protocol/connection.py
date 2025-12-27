# this spec used: https://wiki.theory.org/BitTorrentSpecification

import asyncio
import logging
import struct
import time
from asyncio import StreamReader, StreamWriter, IncompleteReadError
from typing import Tuple, Optional

from .message import Message
from .structures import PeerInfo

logger = logging.getLogger(__name__)

PSTR_V1 = b'BitTorrent protocol'


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
		logger.debug(f"Connection to {peer_info} failed by timeout")
		writer.close()
		return None
	except Exception as ex:
		logger.error(f"TODO: Connection to {peer_info} failed by {ex}")
		writer.close()
		return None

	message = __create_handshake_message(info_hash, local_peer_id, reserved)
	logger.debug(f"Send handshake to: {peer_info}, message: {message}")

	writer.write(message)
	await writer.drain()
	try:
		async with asyncio.timeout(timeout):
			handshake_response = await __read_handshake_message(reader)
	except TimeoutError:
		logger.debug(f"Handshake to {peer_info} failed by timeout")
		writer.close()
		return None
	except IncompleteReadError:
		logger.debug(f"Peer {peer_info} closed the connection.")
		writer.close()
		return None
	except OSError as ex:
		# looks like simple connectin lost.
		logger.debug(f"OSError on {peer_info}. Exception {ex}")
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
		is_timeout = time.monotonic() - self.last_message_time > self.timeout
		return self.reader.at_eof() or self.writer.is_closing() or is_timeout

	def close(self) -> None:
		logger.debug(f"Close connection to {self.remote_peer_id}")
		self.last_message_time = .0
		self.writer.close()

	async def read(self, message_callback) -> bool:
		try:
			buffer = await self.reader.readexactly(4)
			length = struct.unpack("!I", buffer)[0]

			if length:
				buffer = await self.reader.readexactly(length)
				self.last_message_time = time.monotonic()
				message_callback(Message(buffer))
				return True
			else:
				self.last_message_time = time.monotonic()
				return True  # KEEP ALIVE

		except IncompleteReadError as ex:
			logger.debug(f"IncompleteReadError on {self.remote_peer_id}. Exception {ex}")
			return False
		except ConnectionResetError as ex:
			logger.debug(f"ConnectionResetError on {self.remote_peer_id}. Exception {ex}")
			return False
		except OSError as ex:
			# looks like simple connectin lost.
			logger.debug(f"OSError on {self.remote_peer_id}. Exception {ex}")
			return False
		except Exception as ex:
			logger.error(f"Unexpected error on {self.remote_peer_id}. Exception {ex}")
			return False

	async def keep_alive(self) -> None:
		if time.monotonic() - self.last_out_time < 10:
			return
		await self.send(bytes())

	async def send(self, message: bytes) -> None:
		logger.debug(f"send {Message(message)} message to {self.remote_peer_id}")
		try:
			self.last_out_time = time.monotonic()
			self.writer.write(struct.pack("!I", len(message)))
			self.writer.write(message)
			await self.writer.drain()
		except Exception as ex:
			logger.warning(f"got send error on {self.remote_peer_id}: {ex}")
