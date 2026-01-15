import asyncio
import logging
import pickle
import secrets
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Any, List, Tuple, Iterable, Set, Optional

from angelovich.core.DataStorage import Entity

import yap_torrent.dht.connection as dht_connection
from yap_torrent.components.peer_ec import PeerConnectionEC, KnownPeersEC
from yap_torrent.components.torrent_ec import TorrentInfoEC, TorrentEC
from yap_torrent.config import Config
from yap_torrent.dht import bt_dht_messages as msg
from yap_torrent.dht.connection import DHTServerProtocol, DHTServerProtocolHandler, KRPCMessage, KRPCQueryType, \
	KRPCMessageType
from yap_torrent.dht.routing.table import DHTRoutingTable
from yap_torrent.dht.tokens import DHTTokens
from yap_torrent.dht.utils import compact_address, read_compact_node_info, distance
from yap_torrent.env import Env
from yap_torrent.protocol import extensions
from yap_torrent.protocol.connection import Message
from yap_torrent.protocol.extensions import check_extension
from yap_torrent.protocol.structures import PeerInfo
from yap_torrent.system import System

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
		self._my_node_id = load_node_id(self.env.config)

		self._peers: dict[bytes, Set[Tuple[str, int]]] = {}
		self._tokens: DHTTokens = DHTTokens(self.env.external_ip, self.env.config.dht_port)
		self._routing_table = DHTRoutingTable(self._my_node_id, self.BUCKET_CAPACITY)

		self.__server = None

		self.pending_nodes = load_nodes(self.env.config)
		self.extra_good_nodes: Set[Tuple[bytes, str, int]] = set()
		self.bad_nodes: Set[Tuple[str, int]] = set()
		self.pending_torrents: List[bytes] = []

	async def start(self):
		self.env.event_bus.add_listener("peer.connected", self.__on_peer_connected, scope=self)
		self.env.event_bus.add_listener("peer.message", self.__on_message, scope=self)
		self.env.event_bus.add_listener("request.dht.more_peers", self.__on_request_more_peers, scope=self)

		# subscribe to torrents added event
		collection = self.env.data_storage.get_collection(TorrentEC)
		collection.add_listener(collection.EVENT_ADDED, self.__on_torrent_added, self)

		# add torrents without info hash to pending torrents
		for entity in collection.entities:
			if entity.has_component(TorrentInfoEC):
				continue
			self.pending_torrents.append(entity.get_component(TorrentEC).info_hash)

		# start listening for incoming DHT connections
		port = self.env.config.dht_port
		host = self.env.ip
		loop = asyncio.get_running_loop()
		self.__server = await loop.create_datagram_endpoint(
			lambda: DHTServerProtocol(self),
			local_addr=(host, port))

	def close(self):
		self.env.event_bus.remove_all_listeners(self)

		# stop listening for incoming DHT connections
		transport, protocol = self.__server
		transport.close()

		# save nodes from the routing table
		save_nodes(self.env.config, self._routing_table.export_nodes() + self.pending_nodes)

		super().close()

	async def __on_request_more_peers(self, info_hash: bytes):
		self.pending_torrents.append(info_hash)

	async def _update(self, delta_time: float):
		if self.pending_nodes:
			_, host, port = self.pending_nodes.pop(0)
			self.add_task(self._ping_new_host(host, port))
		elif self.pending_torrents:
			info_hash = self.pending_torrents.pop(0)
			self.add_task(self._find_peers(info_hash))
		return await System._update(self, delta_time)

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
		peer_info = peer_entity.get_component(PeerConnectionEC).peer_info
		self._add_node(bytes(), peer_info.host, port)

	async def __on_torrent_added(self, entity: Entity, component: TorrentEC):
		if entity.has_component(TorrentInfoEC):
			return
		self.pending_torrents.append(component.info_hash)

	def _add_node(self, node_id: bytes, host: str, port: int):
		if (host, port) in self.bad_nodes:
			return
		if (node_id, host, port) in self.extra_good_nodes:
			return
		if node_id and (node_id in self._routing_table.nodes):
			return
		self.pending_nodes.append((node_id, host, port))

	async def _find_peers(self, info_hash):
		@dataclass(slots=True)
		class RequestNode:
			node_id: bytes
			host: str
			port: int
			token: bytes = bytes()

		all_nodes: Dict[bytes, RequestNode] = {
			node.id: RequestNode(node.id, node.host, node.port) for node in
			self._routing_table.get_closest_nodes(info_hash, 16)
		}
		done_nodes: Set[bytes] = set()

		found_peers_count = 0
		while True:
			# TODO: move to config
			if found_peers_count > 20:
				break

			# find nodes
			pending_nodes: Set = set(all_nodes.keys()).difference(done_nodes)
			if not pending_nodes:
				break

			# select the closest node to process next
			node_id = min(pending_nodes, key=lambda n: distance(info_hash, n))
			done_nodes.add(node_id)
			request_node = all_nodes[node_id]

			# make a request to peer
			result: Optional[KRPCMessage] = await dht_connection.get_peers(
				self._my_node_id, info_hash, request_node.host, request_node.port)

			# skip in case of errors
			if not result or result.error:
				# TODO: mark node as bad
				continue

			# mark this node as good
			self._routing_table.touch(request_node.node_id, request_node.host, request_node.port)

			r = result.response

			# store token for future use
			request_node.token = r.get("token", bytes())

			# update nodes to search in
			nodes: bytes = r.get("nodes", bytes())
			for node_id, host, port in read_compact_node_info(nodes):
				if node_id not in all_nodes:
					all_nodes[node_id] = RequestNode(node_id, host, port)

			# update peers info for this torrent
			values: List[bytes] = r.get("values", [])
			if values:
				logger.info(f'found {len(values)} peers for {info_hash}')
				found_peers_count += len(values)
				self._update_peers(info_hash, set(PeerInfo.from_bytes(v) for v in values))

		# return torrent to the pending list
		if not found_peers_count:
			self.pending_torrents.append(info_hash)
		# join a swarm
		else:
			join_nodes = sorted((node for node in all_nodes.values() if node.token),
			                    key=lambda n: distance(info_hash, n.node_id))[:self.BUCKET_CAPACITY]
			my_port = self.env.config.dht_port
			for node in join_nodes:
				res = await dht_connection.announce_peer(
					self._my_node_id,
					info_hash,
					node.token,
					my_port,
					node.host,
					node.port
				)
				logger.info(f'announce peer result: {res}')

	# async def update_node_state(self, node: DHTNode):
	# 	logger.info(f'update state of {node}')
	# 	ping_response = await dht_connection.ping(self._my_node_id, node.host, node.port)
	# 	if ping_response:
	# 		node.mark_good()
	# 	else:
	# 		node.mark_fail()

	async def _ping_new_host(self, host: str, port: int) -> None:
		logger.debug('ping sent to %s:%s', host, port)
		ping_response = await dht_connection.ping(self._my_node_id, host, port)

		# no connection to the host or message is broken
		if not ping_response or ping_response.error:
			self.bad_nodes.add((host, port))
			logger.debug('ping failed %s:%s', host, port)
			return

		# the message is broken. retry
		if ping_response.error:
			self.pending_nodes.append((bytes(), host, port))
			logger.error(f'ping {host}:{port} message broken: {ping_response.error}. will retry later')
			return

		# host responded with error. just skip it for now
		if ping_response.message_type == KRPCMessageType.ERROR:
			logger.error(f'ping to {host}:{port} failed with error {ping_response.response_error}')
			return

		remote_node_id = ping_response.response.get("id", bytes())
		if self._routing_table.touch(remote_node_id, host, port):
			logger.debug('new node added: %s', self._routing_table.nodes[remote_node_id])
		else:
			self.extra_good_nodes.add((remote_node_id, host, port))
			logger.debug('no place for new node: %s|%s:%s', remote_node_id, host, port)

	def process_query(self, message: KRPCMessage, addr: tuple[str | Any, int]) -> Dict[str, Any]:
		query_type = message.query_type
		arguments = message.arguments
		if query_type == KRPCQueryType.PING:
			return message.make_response(self._my_node_id, self.query_ping_response(arguments, addr))
		elif query_type == KRPCQueryType.FIND_NODE:
			return message.make_response(self._my_node_id, self.query_find_node_response(arguments, addr))
		elif query_type == KRPCQueryType.GET_PEERS:
			return message.make_response(self._my_node_id, self.query_get_peers_response(arguments, addr))
		elif query_type == KRPCQueryType.ANNOUNCE_PEER:
			if not self._tokens.check(addr[0], arguments["token"]):
				return message.make_error(203, "Bad token")
			return message.make_response(self._my_node_id, self.query_announce_peer_response(arguments, addr))
		return message.make_error(203, "Unknown query type")

	def query_find_node_response(self, arguments: Dict[str, Any], addr: tuple[str | Any, int]) -> Dict[str, Any]:
		target = arguments["target"]
		return {"nodes": self._get_closest_nodes(target)}

	def query_get_peers_response(self, arguments: Dict[str, Any], addr: tuple[str | Any, int]) -> Dict[str, Any]:
		info_hash = arguments["info_hash"]
		result = {}
		values: Iterable[bytes] = self._get_peers(info_hash)
		if values:
			result["values"] = values
		else:
			result["nodes"] = self._get_closest_nodes(info_hash)
		result["token"] = self._tokens.create(addr[0])
		return result

	def query_announce_peer_response(
			self,
			arguments: Dict[str, Any],
			addr: tuple[str | Any, int]) -> Dict[str, Any]:
		host = addr[0]
		implied_port = arguments.get("implied_port", 0)
		port: int = arguments.get("port", 0) if implied_port else addr[1]
		info_hash: bytes = arguments.get("info_hash", bytes())

		self._update_peers(info_hash, {PeerInfo(host, port)})
		return {}

	def query_ping_response(
			self,
			arguments: Dict[str, Any],
			addr: tuple[str | Any, int]) -> Dict[str, Any]:
		# host = addr[0]
		# port = addr[1]
		# node_id = arguments.get("id", bytes())
		# self._add_node(node_id, host, port)
		return {}

	def _update_peers(self, info_hash: bytes, peers: Iterable[PeerInfo]):
		# update local known peers
		self._peers.setdefault(info_hash, set()).update((p.host, p.port) for p in peers)

		# notify the peer system about new peers
		self.env.event_bus.dispatch("peers.update", info_hash, peers)

	def _get_peers(self, info_hash: bytes) -> List[bytes]:
		peers = self._peers.get(info_hash, set())

		torrent = self.env.data_storage.get_collection(TorrentEC).find(info_hash)
		if torrent:
			peers.update(torrent.get_component(KnownPeersEC).peers)

		return list(compact_address(host, port) for host, port in peers)

	def _get_closest_nodes(self, target: bytes) -> bytes:
		nodes = bytearray()
		for node in self._routing_table.get_closest_nodes(target, self.BUCKET_CAPACITY):
			nodes.extend(node.compact_node_info)
		return bytes(nodes)
