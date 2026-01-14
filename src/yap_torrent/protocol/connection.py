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
	logger.debug("try connect to %s", peer_info)
	assert len(reserved) == 8
	assert len(info_hash) == 20
	try:
		async with asyncio.timeout(timeout):
			reader, writer = await asyncio.open_connection(peer_info.host, peer_info.port, local_addr=local_addr)
	except TimeoutError:
		logger.debug("Connection to %s failed by timeout", peer_info)
		return None
	except ConnectionRefusedError as ex:
		logger.debug("Connection to %s Refused. %s", peer_info, ex)
		return None
	except Exception as ex:
		logger.error("TODO: Connection to %s failed by %s", peer_info, ex)
		return None

	message = __create_handshake_message(info_hash, local_peer_id, reserved)
	logger.debug("Send handshake to: %s, message: %s", peer_info, message)

	writer.write(message)
	await writer.drain()
	try:
		async with asyncio.timeout(timeout):
			handshake_response = await __read_handshake_message(reader)
	except TimeoutError:
		logger.debug("Handshake to %s failed by timeout", peer_info)
		return None
	except IncompleteReadError:
		logger.debug("Peer %s closed the connection.", peer_info)
		return None
	except OSError as ex:
		# looks like simple connectin lost.
		logger.debug("OSError on %s. Exception %s", peer_info, ex)
		return None
	except Exception as ex:
		logger.error("Unexpected: Handshake to %s failed by %s", peer_info, ex)
		return None
	# finally:
	# 	writer.close()

	pstrlen, pstr, reserved, remote_info_hash, remote_peer_id = handshake_response
	logger.debug("Received handshake from: %s %s, message: %s", remote_peer_id, peer_info, handshake_response)

	logger.info("Connected to peer: %s. Peer id: %s", peer_info, remote_peer_id)
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
		logger.debug("Incoming handshake timeout error")
		return None
	except IncompleteReadError as ex:
		logger.debug("Incoming handshake connection error %s", ex)
		return None
	except ConnectionResetError as ex:
		logger.debug("Incoming handshake connection error %s", ex)
		return None
	except Exception as ex:
		logger.error("Incoming handshake unexpected error %s", ex)
		return None
	# finally:
	# 	writer.close()

	try:
		message = __create_handshake_message(info_hash, local_peer_id, reserved)
		logger.debug("Send handshake back to: %s, message: %s", remote_peer_id, message)
		writer.write(message)
		await writer.drain()
	except Exception as ex:
		logger.error("Handshake to %s failed by %s", remote_peer_id, ex)
		return None
	# finally:
	# 	writer.close()

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
		logger.debug("Close connection to %s", self.remote_peer_id)
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
			logger.debug("Message read failed on Peer [%s]. Exception %s", self.remote_peer_id, ex)
			return False
		except ConnectionResetError as ex:
			logger.debug("Message read failed on Peer [%s]. Exception %s", self.remote_peer_id, ex)
			return False
		except OSError as ex:
			# looks like simple connectin lost.
			logger.debug("Message read failed on Peer [%s]. Exception %s", self.remote_peer_id, ex)
			return False
		except Exception as ex:
			logger.error("Unexpected error on Peer [%s]. Exception %s", self.remote_peer_id, ex)
			return False

	async def keep_alive(self) -> None:
		if time.monotonic() - self.last_out_time < 10:
			return
		await self.send(bytes())

	async def send(self, message: bytes) -> None:

		if self.writer.is_closing():
			return

		logger.debug("send %s message to %s", Message(message), self.remote_peer_id)
		try:
			self.last_out_time = time.monotonic()
			self.writer.write(struct.pack("!I", len(message)))
			self.writer.write(message)
			await self.writer.drain()
		except ConnectionResetError as ex:
			logger.debug("Connection lost %s", ex)
		except ConnectionAbortedError as ex:
			logger.debug("Connection lost %s", ex)
		except Exception as ex:
			logger.error("got send error on %s: %s", self.remote_peer_id, ex)
