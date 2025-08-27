import asyncio
import logging
import pickle
import secrets
from pathlib import Path
from typing import Dict, Any, List, Tuple, Optional

import torrent_app.dht.connection as dht_connection
from angelovichcore.DataStorage import Entity
from torrent_app import System, Env, Config
from torrent_app.components.peer_ec import PeerInfoEC, PeerConnectionEC
from torrent_app.dht.connection import DHTServerProtocol, DHTServerProtocolHandler, make_response
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


class DHTSystem(System, DHTServerProtocolHandler):
	def __init__(self, env: Env):
		super().__init__(env)
		self.__my_node_id = load_node_id(self.env.config)

		self.__tokens: DHTTokens = DHTTokens(self.env.external_ip, self.env.config.dht_port)
		self.__routing_table = DHTRoutingTable(self.__my_node_id)
		self.__peers: Dict[bytes, List[bytes]] = {}

		self.__server = None

	async def start(self) -> 'System':
		self.env.event_bus.add_listener("torrent.peer.ready", self.__on_peer_ready, scope=self)
		self.env.event_bus.add_listener("torrent.peer.message.port", self.__on_torrent_message, scope=self)

		port = self.env.config.dht_port
		host = self.env.ip

		loop = asyncio.get_running_loop()
		self.__server = await loop.create_datagram_endpoint(
			lambda: DHTServerProtocol(self),
			local_addr=(host, port))

		return await super().start()

	def close(self):
		transport, protocol = self.__server
		transport.close()
		super().close()

	async def _update(self, delta_time: float):
		return await super()._update(delta_time)

	def create_response(self, message: Dict[str, Any], addr: tuple[str | Any, int]) -> Dict[str, Any]:
		t = message.get("t")

		# "y" key is one of "q" for query, "r" for response, or "e" for error
		message_type = message.get("y")
		if message_type == "q":
			query_type = message.get("q")
			if query_type == "ping":
				return make_response(t, self.__my_node_id, response={})
			elif query_type == "find_node":
				return make_response(t, self.__my_node_id, *self.find_node_response(message))
			elif query_type == "get_peers":
				return make_response(t, self.__my_node_id, *self.get_peers_response(message, addr))
			elif query_type == "announce_peer":
				return make_response(t, self.__my_node_id, *self.announce_peer_response(message, addr))
			else:
				return make_response(t, error=(204, f"query type {query_type} Unknown"))
		else:
			return make_response(t, error=(204, f"Method '{message_type}' Unknown"))

	async def __on_peer_ready(self, torrent_entity: Entity, peer_entity: Entity) -> None:
		peer_connection_ec = peer_entity.get_component(PeerConnectionEC)
		reserved = peer_connection_ec.reserved
		if not check_extension(reserved, extensions.DHT):
			return

		# send port message to connected peer
		await peer_connection_ec.connection.port(self.env.config.dht_port)

	async def __on_torrent_message(self, peer_entity: Entity, message: Message) -> None:
		peer_info = peer_entity.get_component(PeerInfoEC).peer_info
		await self.ping_host(peer_info.host, message.port)

	async def ping_host(self, host: str, port: int) -> None:
		logger.info(f'ping sent to {host}:{port}')
		response = await dht_connection.ping(self.__my_node_id, host, port)
		if response:
			remote_node_id = response.get("r", {}).get("id", bytes())
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

	def find_node_response(self, message: Dict[str, Any]) -> Tuple[Optional[Tuple], Dict[str, Any]]:
		arguments = message.get("a", {})
		target: bytes = arguments.get("target")
		return (), {"nodes": self.__routing_table.get_closest_nodes(target)}

	def get_peers_response(self, message: Dict[str, Any], addr: tuple[str | Any, int]) -> Tuple[
		Optional[Tuple], Dict[str, Any]]:
		arguments = message.get("a", {})
		info_hash: bytes = arguments.get("info_hash")
		result = {}

		values: List[bytes] = self.__peers.get(info_hash, [])
		if values:
			result["values"] = values
		else:
			result["nodes"] = self.__routing_table.get_closest_nodes(info_hash)
		result["token"] = self.__tokens.create(addr[0])
		return None, result

	def announce_peer_response(self,
	                           message: Dict[str, Any],
	                           addr: tuple[str | Any, int]) -> Tuple[Optional[Tuple], Dict[str, Any]]:
		arguments = message.get("a", {})

		token = arguments.get("token")
		if not self.__tokens.check(addr[0], token):
			return (203, "Bad token"), {}

		host = addr[0]
		implied_port = arguments.get("implied_port", 0)
		port = arguments.get("port") if implied_port else addr[1]
		info_hash = arguments.get("info_hash")
		self.__peers.setdefault(info_hash, []).append(compact_address(host, port))

		return None, {}
