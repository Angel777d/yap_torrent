import asyncio
import logging
import pickle
import secrets
from pathlib import Path
from typing import Dict, Any, List, Tuple, Iterable, Set

import torrent_app.dht.connection as dht_connection
from angelovichcore.DataStorage import Entity
from torrent_app import System, Env, Config
from torrent_app.components.peer_ec import PeerInfoEC, PeerConnectionEC, KnownPeersEC, KnownPeersUpdateEC
from torrent_app.components.torrent_ec import TorrentInfoEC, TorrentHashEC
from torrent_app.dht import bt_dht_messages as msg
from torrent_app.dht.connection import DHTServerProtocol, DHTServerProtocolHandler, KRPCMessage, KRPCQueryType, \
	KRPCMessageType
from torrent_app.dht.routing.table import DHTRoutingTable
from torrent_app.dht.tokens import DHTTokens
from torrent_app.dht.utils import compact_address, read_compact_node_info, distance
from torrent_app.protocol import extensions
from torrent_app.protocol.connection import Message
from torrent_app.protocol.extensions import check_extension
from torrent_app.protocol.structures import PeerInfo

logger = logging.getLogger(__name__)


def get_path_checked(config: Config) -> Path:
	file_path = Path(config.data_folder).joinpath("dht")
	file_path.mkdir(parents=True, exist_ok=True)
	return file_path


def load_node_id(config: Config) -> bytes:
	file_path = get_path_checked(config).joinpath("node_id")
	if file_path.exists():
		with open(file_path, "rb") as f:
			node_id: bytes = pickle.load(f)
	else:
		node_id: bytes = secrets.token_bytes(20)
		with open(file_path, "wb") as f:
			pickle.dump(node_id, f)
	return node_id


def load_nodes(config: Config) -> List[Tuple[bytes, str, int]]:
	file_path = get_path_checked(config).joinpath("peers")
	if not file_path.exists():
		return []

	with open(file_path, "rb") as f:
		return pickle.load(f)


def save_nodes(config: Config, peers: List[Tuple[bytes, str, int]]):
	file_path = get_path_checked(config).joinpath("peers")

	with open(file_path, "wb") as f:
		pickle.dump(peers, f)


class BTDHTSystem(System, DHTServerProtocolHandler):
	BUCKET_CAPACITY = 8

	def __init__(self, env: Env):
		super().__init__(env)
		self.__my_node_id = load_node_id(self.env.config)

		self.__tokens: DHTTokens = DHTTokens(self.env.external_ip, self.env.config.dht_port)
		self.__routing_table = DHTRoutingTable(self.__my_node_id, self.BUCKET_CAPACITY)

		self.__server = None

		self.pending_nodes = load_nodes(self.env.config)
		self.extra_good_nodes: Set[Tuple[bytes, str, int]] = set()
		self.bad_nodes: Set[Tuple[str, int]] = set()
		self.pending_torrents: List[bytes] = []

	async def start(self):
		self.env.event_bus.add_listener("peer.connected", self.__on_peer_connected, scope=self)
		self.env.event_bus.add_listener("peer.message", self.__on_message, scope=self)

		# subscribe to torrents added event
		collection = self.env.data_storage.get_collection(TorrentHashEC)
		collection.add_listener(collection.EVENT_ADDED, self.__on_torrent_added, self)

		# add torrents without info hash to pending torrents
		for entity in collection.entities:
			if entity.has_component(TorrentInfoEC):
				continue
			self.pending_torrents.append(entity.get_component(TorrentHashEC).info_hash)

		# start listening for incoming DHT connections
		port = self.env.config.dht_port
		host = self.env.ip

		loop = asyncio.get_running_loop()
		self.__server = await loop.create_datagram_endpoint(
			lambda: DHTServerProtocol(self),
			local_addr=(host, port))

	def close(self):
		self.env.event_bus.remove_all_listeners(self)

		transport, protocol = self.__server
		transport.close()

		# save nodes from the routing table
		save_nodes(self.env.config, self.__routing_table.export_nodes() + self.pending_nodes)

		System.close(self)

	async def _update(self, delta_time: float):
		if self.pending_nodes:
			_, host, port = self.pending_nodes.pop(0)
			self.add_task(self._ping_new_host(host, port))
		elif self.pending_torrents:
			info_hash = self.pending_torrents.pop(0)
			self.add_task(self.__get_peers_for(info_hash))
		return await System._update(self, delta_time)

	def process_query(self, message: KRPCMessage, addr: tuple[str | Any, int]) -> Dict[str, Any]:
		query_type = message.query_type
		arguments = message.arguments
		if query_type == KRPCQueryType.PING:
			return message.make_response(self.__my_node_id, response=self.ping_response(arguments, addr))
		elif query_type == KRPCQueryType.FIND_NODE:
			return message.make_response(self.__my_node_id, self.find_node_response(arguments, addr))
		elif query_type == KRPCQueryType.GET_PEERS:
			return message.make_response(self.__my_node_id, self.get_peers_response(arguments, addr))
		elif query_type == KRPCQueryType.ANNOUNCE_PEER:
			if not self.__tokens.check(addr[0], arguments["token"]):
				return message.make_error(203, "Bad token")
			return message.make_response(self.__my_node_id, self.announce_peer_response(arguments, addr))
		return message.make_error(203, "Unknown query type")

	async def __on_peer_connected(self, _: Entity, peer_entity: Entity) -> None:
		peer_connection_ec = peer_entity.get_component(PeerConnectionEC)
		reserved = peer_connection_ec.reserved
		if not check_extension(reserved, extensions.DHT):
			return

		# send a port message to a connected peer
		await peer_connection_ec.connection.send(msg.port(self.env.config.dht_port))

	async def __on_message(self, _: Entity, peer_entity: Entity, message: Message):
		if message.message_id != msg.PORT:
			return

		port = msg.payload_port(message)
		peer_info = peer_entity.get_component(PeerInfoEC).peer_info
		self._add_node(bytes(), peer_info.host, port)

	async def __on_torrent_added(self, entity: Entity, component: TorrentHashEC):
		if entity.has_component(TorrentInfoEC):
			return
		self.pending_torrents.append(component.info_hash)

	def _add_node(self, node_id: bytes, host: str, port: int):
		if (host, port) in self.bad_nodes:
			return
		if (node_id, host, port) in self.extra_good_nodes:
			return
		if node_id and (node_id in self.__routing_table.nodes):
			return
		self.pending_nodes.append((node_id, host, port))

	async def __get_peers_for(self, info_hash):
		done_nodes = set()
		nodes_to_ask: set[Tuple[bytes, str, int]] = set(
			(node.id, node.host, node.port) for node in self.__routing_table.get_closest_nodes(info_hash, 16))
		found_peers_count = 0
		while nodes_to_ask:
			if found_peers_count > self.BUCKET_CAPACITY * 2:
				return
			found_peers_count += 1
			node_id, host, port = min(nodes_to_ask, key=lambda n: distance(info_hash, n[0]))
			nodes_to_ask.remove((node_id, host, port))
			if node_id in done_nodes:
				continue
			done_nodes.add(node_id)
			res = await dht_connection.get_peers(self.__my_node_id, info_hash, host, port)
			if not res:
				continue

			r = res.response
			token: bytes = r.get("token", bytes())

			nodes: bytes = r.get("nodes", bytes())
			if nodes:
				for node_id, host, port in read_compact_node_info(nodes):
					nodes_to_ask.add((node_id, host, port))
					self._add_node(node_id, host, port)

			values: List[bytes] = r.get("values", [])
			# update peers info for this torrent
			if values:
				logger.info(f'found {values} peers for {info_hash}')
				found_peers_count += len(values)
				self._update_peers(info_hash, set(PeerInfo.from_bytes(v) for v in values))

		# return torrent to the pending list
		if not found_peers_count:
			self.pending_torrents.append(info_hash)

	# async def update_node_state(self, node: DHTNode):
	# 	logger.info(f'update state of {node}')
	# 	ping_response = await dht_connection.ping(self.__my_node_id, node.host, node.port)
	# 	if ping_response:
	# 		node.mark_good()
	# 	else:
	# 		node.mark_fail()

	async def _ping_new_host(self, host: str, port: int) -> None:
		logger.debug(f'ping sent to {host}:{port}')
		ping_response = await dht_connection.ping(self.__my_node_id, host, port)

		# no connection to the host or message is broken
		if not ping_response or ping_response.error:
			self.bad_nodes.add((host, port))
			logger.debug(f'ping failed {host}:{port}')
			return

		# host responded with error
		if ping_response.message_type == KRPCMessageType.ERROR:
			logger.error(f'ping to {host}:{port} failed with error {ping_response.response_error}')

		remote_node_id = ping_response.response.get("id", bytes())
		if self.__routing_table.touch(remote_node_id, host, port):
			logger.debug(f'new node added: {self.__routing_table.nodes[remote_node_id]}')
		else:
			self.extra_good_nodes.add((remote_node_id, host, port))
			logger.debug(f'no place for new node: {remote_node_id}|{host}:{port}')

	def find_node_response(self, arguments: Dict[str, Any], addr: tuple[str | Any, int]) -> Dict[str, Any]:
		target = arguments["target"]
		return {"nodes": self._get_closest_nodes(target)}

	def get_peers_response(self, arguments: Dict[str, Any], addr: tuple[str | Any, int]) -> Dict[str, Any]:
		info_hash = arguments["info_hash"]
		result = {}
		values: Iterable[bytes] = self._get_peers(info_hash)
		if values:
			result["values"] = values
		else:
			result["nodes"] = self._get_closest_nodes(info_hash)
		result["token"] = self.__tokens.create(addr[0])
		return result

	def announce_peer_response(self,
	                           arguments: Dict[str, Any],
	                           addr: tuple[str | Any, int]) -> Dict[str, Any]:
		host = addr[0]
		implied_port = arguments.get("implied_port", 0)
		port: int = arguments.get("port", 0) if implied_port else addr[1]
		info_hash: bytes = arguments.get("info_hash", bytes())

		# TODO: what if i don't have this torrent?
		self._update_peers(info_hash, {PeerInfo(host, port)})

		return {}

	def ping_response(self,
	                  arguments: Dict[str, Any],
	                  addr: tuple[str | Any, int]) -> Dict[str, Any]:
		host = addr[0]
		port = addr[1]
		node_id = arguments.get("id", bytes())
		self._add_node(node_id, host, port)
		return {}

	def _update_peers(self, info_hash: bytes, values: Iterable[PeerInfo]):
		ds = self.env.data_storage
		torrent_entity = ds.get_collection(TorrentHashEC).find(info_hash)
		if not torrent_entity:
			logger.error(f"There is no torrent with info hash {info_hash}")
			return
		torrent_entity.get_component(KnownPeersEC).update_peers(values)
		torrent_entity.add_component(KnownPeersUpdateEC())

	def _get_peers(self, info_hash: bytes) -> List[bytes]:
		torrent = self.env.data_storage.get_collection(TorrentHashEC).find(info_hash)
		if not torrent:
			return []

		peers_ec: KnownPeersEC = torrent.get_component(KnownPeersEC)
		if not peers_ec:
			return []

		return list(compact_address(peer.host, peer.port) for peer in peers_ec.peers)

	def _get_closest_nodes(self, target: bytes) -> bytes:
		nodes = bytearray()
		for node in self.__routing_table.get_closest_nodes(target, self.BUCKET_CAPACITY):
			nodes.extend(node.compact_node_info)
		return bytes(nodes)
