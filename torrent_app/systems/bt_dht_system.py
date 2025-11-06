import asyncio
import logging
import pickle
import secrets
from pathlib import Path
from typing import Dict, Any, List

import torrent_app.dht.connection as dht_connection
from angelovichcore.DataStorage import Entity
from torrent_app import System, Env, Config
from torrent_app.components.peer_ec import PeerInfoEC, PeerConnectionEC
from torrent_app.dht import bt_dht_messages as msg
from torrent_app.dht.connection import DHTServerProtocol, DHTServerProtocolHandler, make_response, make_error
from torrent_app.dht.nodes import DHTNode
from torrent_app.dht.routing import DHTRoutingTable
from torrent_app.dht.tokens import DHTTokens
from torrent_app.dht.utils import compact_address, read_compact_node_info
from torrent_app.protocol import extensions
from torrent_app.protocol.connection import Message
from torrent_app.protocol.extensions import check_extension

logger = logging.getLogger(__name__)


def load_node_id(config: Config) -> bytes:
	file_path = Path(config.data_folder)
	file_path.mkdir(parents=True, exist_ok=True)
	file_path = file_path.joinpath("node_id")
	if file_path.exists():
		with open(file_path, "rb") as f:
			node_id: bytes = pickle.load(f)
	else:
		node_id: bytes = secrets.token_bytes(20)
		with open(file_path, "wb") as f:
			pickle.dump(node_id, f)
	return node_id


class BTDHTSystem(System, DHTServerProtocolHandler):
	def __init__(self, env: Env):
		super().__init__(env)
		self.__my_node_id = load_node_id(self.env.config)

		self.__tokens: DHTTokens = DHTTokens(self.env.external_ip, self.env.config.dht_port)
		self.__routing_table = DHTRoutingTable(self.__my_node_id)
		self.__torrent_peers: Dict[bytes, List[bytes]] = {}

		self.__server = None

	async def start(self):
		self.env.event_bus.add_listener("peer.connected", self.__on_peer_connected, scope=self)
		self.env.event_bus.add_listener("peer.message", self.__on_message, scope=self)

		# TODO: load local data

		port = self.env.config.dht_port
		host = self.env.ip

		loop = asyncio.get_running_loop()
		self.__server = await loop.create_datagram_endpoint(
			lambda: DHTServerProtocol(self),
			local_addr=(host, port))

	def close(self):
		transport, protocol = self.__server
		transport.close()

		# TODO: save local data: routing table, peers and tokens
		System.close(self)

	async def _update(self, delta_time: float):
		return await System._update(self, delta_time)

	def process_message(self, message: Dict[str, Any], addr: tuple[str | Any, int]) -> Dict[str, Any]:
		t: str = message.get("t", "")

		# "y" key is one of "q" for query, "r" for response, or "e" for error
		message_type = message.get("y")
		if message_type == "q":
			query_type = message.get("q")
			arguments = message.get("a", {})
			if query_type == "ping":
				return make_response(t, self.__my_node_id, response={})
			elif query_type == "find_node":
				if not arguments:
					return make_error(t, 203, "Missing arguments")
				if "target" not in arguments:
					return make_error(t, 203, "Missing target argument")
				return make_response(t, self.__my_node_id, self.find_node_response(arguments["target"]))
			elif query_type == "get_peers":
				if not arguments:
					return make_error(t, 203, "Missing arguments")
				if "info_hash" not in arguments:
					return make_error(t, 203, "Missing info_hash argument")
				return make_response(t, self.__my_node_id, self.get_peers_response(arguments["info_hash"], addr))
			elif query_type == "announce_peer":
				if not arguments:
					return make_error(t, 203, "Missing arguments")
				if "info_hash" not in arguments:
					return make_error(t, 203, "Missing info_hash argument")
				if "token" not in arguments:
					return make_error(t, 203, "Missing token argument")
				if not self.__tokens.check(addr[0], arguments["token"]):
					return make_error(t, 203, "Bad token")
				return make_response(t, self.__my_node_id, self.announce_peer_response(message, addr))
			else:
				return make_error(t, 204, f"query type {query_type} Unknown")
		else:
			return make_error(t, 204, f"Method '{message_type}' Unknown")

	async def __on_peer_connected(self, _: Entity, peer_entity: Entity) -> None:
		peer_connection_ec = peer_entity.get_component(PeerConnectionEC)
		reserved = peer_connection_ec.reserved
		if not check_extension(reserved, extensions.DHT):
			return

		# send port message to connected peer
		await peer_connection_ec.connection.send(msg.port(self.env.config.dht_port))

	async def __on_message(self, _: Entity, peer_entity: Entity, message: Message):
		if message.message_id != msg.PORT:
			return

		port = msg.payload_port(message)
		peer_info = peer_entity.get_component(PeerInfoEC).peer_info
		await self.ping_host(peer_info.host, port)

	async def ping_host(self, host: str, port: int) -> None:
		logger.info(f'ping sent to {host}:{port}')
		ping_response = await dht_connection.ping(self.__my_node_id, host, port)
		if ping_response:
			remote_node_id = ping_response.get("r", {}).get("id", bytes())
			self.add_node(remote_node_id, host, port)

			target = self.__my_node_id[:-1] + b'0'
			res = await dht_connection.find_node(self.__my_node_id, target, host, port)
			nodes = res.get("r", {}).get("nodes", bytes())
			for node_id, host, port in read_compact_node_info(nodes):
				self.add_node(node_id, host, port)
		else:
			# TODO: ignore?
			logger.info(f'ping failed {host}:{port}')

	def add_node(self, remote_node_id: bytes, host: str, port: int) -> None:

		if remote_node_id in self.__routing_table.nodes:
			logger.debug(f'node {remote_node_id} already added to routing table')
			return

		node = DHTNode(remote_node_id, host, port)
		node.mark_good()
		logger.info(f'add {node} to routing table')
		self.__routing_table.add_node(node)

	def find_node_response(self, target: bytes) -> Dict[str, Any]:
		return {"nodes": self.__routing_table.get_closest_nodes(target)}

	def get_peers_response(self, info_hash: bytes, addr: tuple[str | Any, int]) -> Dict[str, Any]:
		result = {}
		values: List[bytes] = self.__torrent_peers.get(info_hash, [])
		if values:
			result["values"] = values
		else:
			result["nodes"] = self.__routing_table.get_closest_nodes(info_hash)
		result["token"] = self.__tokens.create(addr[0])
		return result

	def announce_peer_response(self,
	                           arguments: Dict[str, Any],
	                           addr: tuple[str | Any, int]) -> Dict[str, Any]:
		host = addr[0]
		implied_port = arguments.get("implied_port", 0)
		port: int = arguments.get("port", 0) if implied_port else addr[1]
		info_hash: bytes = arguments.get("info_hash", bytes())
		self.__torrent_peers.setdefault(info_hash, []).append(compact_address(host, port))

		return {}
