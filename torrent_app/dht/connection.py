import asyncio
import logging
import random
from asyncio import DatagramProtocol, transports
from typing import Any, Optional, Dict, Tuple

from torrent_app.protocol import encode, decode

logger = logging.getLogger(__name__)

CLIENT_VERSION = "AP"


class DHTServerProtocolHandler:
	def create_response(self, message: Dict[str, Any], addr: tuple[str | Any, int]) -> Dict[str, Any]:
		raise NotImplementedError()


class DHTServerProtocol(DatagramProtocol):
	def __init__(self, handler: DHTServerProtocolHandler) -> None:
		self.handler = handler
		self.transport = None

	def connection_made(self, transport: transports.DatagramTransport):
		self.transport = transport
		logger.info('some DHT node connected to us')

	def datagram_received(self, data: bytes, addr: tuple[str | Any, int]):
		message = decode(data)
		logger.info(f'got some DHT message {data} from addr {addr}')
		response = self.handler.create_response(message)
		self.transport.sendto(encode(response))
		self.transport.close()


class DHTClientProtocol(DatagramProtocol):
	def __init__(self, message: bytes, on_con_lost):
		self.message = message
		self.on_con_lost = on_con_lost

		self.transport = None

		self.response = bytes()
		self.addr = None

	def connection_made(self, transport):
		logger.debug("DHT client connection made")
		self.transport = transport
		self.transport.sendto(self.message)

	def datagram_received(self, data: bytes, addr: tuple[str | Any, int]):
		logger.debug("DHT client connection data received")
		self.response = data
		self.addr = addr
		self.transport.close()

	def error_received(self, exc):
		logger.debug(f"Error received: {exc}")

	def connection_lost(self, exc):
		logger.debug("Connection closed")
		self.on_con_lost.set_result(True)


async def __send_message(message: Dict[str, Any], host: str, port: int) -> Optional[Dict[str, Any]]:
	message["v"] = CLIENT_VERSION
	loop = asyncio.get_running_loop()
	on_con_lost = loop.create_future()
	transport, protocol = await loop.create_datagram_endpoint(
		lambda: DHTClientProtocol(encode(message), on_con_lost),
		remote_addr=(host, port)
	)
	try:
		await on_con_lost
	finally:
		transport.close()

	if protocol.response:
		return decode(protocol.response)
	return None


def __get_transaction_id() -> str:
	return random.choice("abcdefg") + random.choice("zyx")


async def announce_peer(
		node_id: bytes,
		implied_port: bool,
		info_hash: bytes,
		p: int,
		token: bytes,
		host: str,
		port: int) -> Optional[Dict[str, Any]]:
	return await __send_message({
		"t": __get_transaction_id(),
		"y": "q",
		"q": "announce_peer",
		"a": {
			"id": node_id,
			"implied_port": int(implied_port),
			"info_hash": info_hash,
			"port": p,
			"token": token,
		}
	}, host, port)


async def get_peers(node_id: bytes, info_hash: bytes, host: str, port: int) -> Optional[Dict[str, Any]]:
	return await __send_message({
		"t": __get_transaction_id(),
		"y": "q",
		"q": "get_peers",
		"a": {
			"id": node_id,
			"info_hash": info_hash
		}
	}, host, port)


async def find_node(node_id: bytes, target: bytes, host: str, port: int) -> Optional[Dict[str, Any]]:
	return await __send_message({
		"t": __get_transaction_id(),
		"y": "q",
		"q": "find_node",
		"a": {
			"id": node_id,
			"target": target
		}
	}, host, port)


async def ping(node_id: bytes, host: str, port: int) -> Optional[Dict[str, Any]]:
	return await __send_message({
		"t": __get_transaction_id(),
		"y": "q",
		"q": "ping",
		"a": {
			"id": node_id,
		}
	}, host, port)


def make_response(t: str,
                  node_id: Optional[bytes] = None,
                  error: Optional[Tuple[int, str]] = None,
                  response: Optional[Dict[str, Any]] = None):
	# prepare an error
	if error:
		return {
			"t": t,
			"y": "e",
			"e": error
		}

	# prepare response structure
	response["id"] = node_id
	return {
		"t": t,
		"y": "r",
		"r": response
	}
