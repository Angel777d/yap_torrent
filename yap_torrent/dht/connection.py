import asyncio
import logging
import random
from asyncio import DatagramProtocol, transports
from enum import Enum
from typing import Any, Optional, Dict

from yap_torrent.protocol import encode, decode

logger = logging.getLogger(__name__)

CLIENT_VERSION = "AP"


# "y" key is one of "q" for a query, "r" for response, or "e" for error
class KRPCMessageType(Enum):
	QUERY = "q"
	RESPONSE = "r"
	ERROR = "e"


class KRPCQueryType(Enum):
	PING = "ping"
	FIND_NODE = "find_node"
	GET_PEERS = "get_peers"
	ANNOUNCE_PEER = "announce_peer"


class KRPCMessage:
	def __init__(self, data: Dict[str, Any]):
		self._data = data
		self.t: bytes = data.get("t", b'')

		self.error = None
		self.message_type = None
		self.query_type = None

		message_type = data.get("y").decode("utf-8")
		try:
			self.message_type = KRPCMessageType(message_type)
		except ValueError:
			self.error = self.make_error(202, f"Message type '{message_type}' Unknown.")
			return

		if self.message_type == KRPCMessageType.QUERY:
			query_type = data.get(message_type).decode("utf-8")
			try:
				self.query_type = KRPCQueryType(query_type)
			except ValueError:
				self.error = self.make_error(204, f"Method '{query_type}' Unknown.")
				return

			arguments = data.get("a", {})
			if arguments:
				to_check = {
					KRPCQueryType.PING: ("id",),
					KRPCQueryType.FIND_NODE: ("id", "target"),
					KRPCQueryType.GET_PEERS: ("id", "info_hash"),
					KRPCQueryType.ANNOUNCE_PEER: ("id", "info_hash", "token"),
				}

				for field in to_check[self.query_type]:
					if field not in arguments:
						self.error = self.make_error(203, f"Missing '{field}' argument")
						return
			else:
				self.error = self.make_error(203, f"Missing arguments value")
				return
		elif self.message_type == KRPCMessageType.RESPONSE:
			response = data.get("r", {})
			if not response:
				self.error = self.make_error(203, f"Missing response value")
				return
		elif self.message_type == KRPCMessageType.RESPONSE:
			error = data.get("e", {})
			if not error:
				self.error = self.make_error(203, f"Missing error value")
				return

	@property
	def arguments(self):
		if self.message_type != KRPCMessageType.QUERY:
			raise ValueError("This message is not a query.")
		return self._data.get("a", {})

	@property
	def response(self) -> Dict[str, Any]:
		if self.message_type != KRPCMessageType.RESPONSE:
			raise ValueError("This message is not a response.")
		return self._data.get("r", {})

	@property
	def response_error(self):
		if self.message_type != KRPCMessageType.ERROR:
			raise ValueError("This message is not a error.")
		return self._data.get("e", {})

	def make_response(self,
	                  node_id: bytes,
	                  response: Dict[str, Any]):
		# prepare response structure
		response["id"] = node_id
		return {
			"t": self.t,
			"y": KRPCMessageType.RESPONSE.value,
			"r": response
		}

	def make_error(self, code: int, error_message: str):
		return {
			"t": self.t,
			"y": KRPCMessageType.ERROR.value,
			"e": (code, error_message)
		}

	def __repr__(self):
		return f"KRPCMessage({self.message_type}, {self._data}, {self.error})"


class DHTServerProtocolHandler:
	def process_query(self, message: KRPCMessage, addr: tuple[str | Any, int]) -> Dict[str, Any]:
		raise NotImplementedError()


class DHTServerProtocol(DatagramProtocol):
	def __init__(self, handler: DHTServerProtocolHandler) -> None:
		self.handler = handler
		self.transport = None

	def connection_made(self, transport: transports.DatagramTransport):
		self.transport = transport
		logger.debug('some DHT node connected to us')

	def datagram_received(self, data: bytes, addr: tuple[str | Any, int]):
		# logger.debug(f'got KRPCMessage {data} from addr {addr}')
		try:
			decoded = decode(data)
		except Exception as ex:
			logger.error(f'DHT message {data} from addr {addr} failed to decode. {ex}')
			self.transport.close()
			return

		message = KRPCMessage(decoded)
		if message.error:
			send_data = encode(message.error)
		else:
			send_data = encode(self.process_query(message, addr))
		self.transport.sendto(send_data, addr)
		self.transport.close()

	def process_query(self, message: KRPCMessage, addr):
		if message.message_type != KRPCMessageType.QUERY:
			return message.make_error(202, f"Message type '{message.message_type.name}' is not expected.")

		return self.handler.process_query(message, addr)


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


async def __send_message(message: Dict[str, Any], host: str, port: int, timeout=2) -> Optional[KRPCMessage]:
	message["v"] = CLIENT_VERSION
	loop = asyncio.get_running_loop()
	on_con_lost = loop.create_future()
	transport, protocol = await loop.create_datagram_endpoint(
		lambda: DHTClientProtocol(encode(message), on_con_lost),
		remote_addr=(host, port)
	)

	try:
		async with asyncio.timeout(timeout):
			await on_con_lost
	except TimeoutError:
		logger.debug("Message %s to %s:%s failed by timeout", message, host, port)
		return None

	if protocol.response:
		try:
			return KRPCMessage(decode(protocol.response))
		except Exception as ex:
			logger.error(f"Failed to decode response from {host}:{port}. {protocol.response} {ex}")
	return None


def __get_transaction_id() -> str:
	return random.choice("abcdefg") + random.choice("zyx")


async def announce_peer(
		node_id: bytes,
		info_hash: bytes,
		token: bytes,
		my_port: int,
		host: str,
		port: int,
) -> Optional[KRPCMessage]:
	args: Dict[str, Any] = {
		"id": node_id,
		"info_hash": info_hash,
		"token": token,
		"port": my_port
	}

	if not my_port:
		args["implied_port"] = 1

	return await __send_message({
		"t": __get_transaction_id(),
		"y": "q",
		"q": "announce_peer",
		"a": args
	}, host, port)


async def get_peers(node_id: bytes, info_hash: bytes, host: str, port: int) -> Optional[KRPCMessage]:
	return await __send_message({
		"t": __get_transaction_id(),
		"y": "q",
		"q": "get_peers",
		"a": {
			"id": node_id,
			"info_hash": info_hash
		}
	}, host, port)


async def find_node(node_id: bytes, target: bytes, host: str, port: int) -> Optional[KRPCMessage]:
	return await __send_message({
		"t": __get_transaction_id(),
		"y": "q",
		"q": "find_node",
		"a": {
			"id": node_id,
			"target": target
		}
	}, host, port)


async def ping(node_id: bytes, host: str, port: int) -> Optional[KRPCMessage]:
	return await __send_message({
		"t": __get_transaction_id(),
		"y": "q",
		"q": "ping",
		"a": {
			"id": node_id,
		}
	}, host, port)
