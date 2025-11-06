from typing import List, Dict, Tuple

from torrent_app.dht.nodes import DHTNodeState, DHTNode
from torrent_app.dht.utils import bytes_to_int, distance


class DHTBucket:
	BUCKET_CAPACITY = 8
	FULL_NODES_RANGE = 2 ** 160

	def __init__(self, min_node: int = 0, max_node: int = FULL_NODES_RANGE):
		self.min_node: int = min_node
		self.max_node: int = max_node
		self.nodes: List[bytes] = []

	def is_suitable(self, node: bytes) -> bool:
		if len(node) == 20:
			return self.min_node <= bytes_to_int(node) < self.max_node
		return False

	def is_full(self) -> bool:
		return len(self.nodes) >= self.BUCKET_CAPACITY

	def add_node(self, node: bytes) -> None:
		if self.is_full():
			raise RuntimeError("bucket full. can't add node")
		self.nodes.append(node)

	def split(self) -> Tuple["DHTBucket", "DHTBucket"]:
		mid = self.min_node + (self.max_node - self.min_node) // 2
		result = (DHTBucket(self.min_node, mid), DHTBucket(mid, self.max_node))
		for node in self.nodes:
			for b in result:
				if b.is_suitable(node):
					b.add_node(node)
					break
		return result

	def can_split(self):
		return self.max_node - self.min_node > self.BUCKET_CAPACITY


class DHTRoutingTable:
	def __init__(self, local_node_id: bytes):
		self.buckets: List[DHTBucket] = [DHTBucket()]
		self.local_node_id = local_node_id
		self.nodes: Dict[bytes, DHTNode] = {}

	def add_node(self, node: DHTNode) -> None:
		# check the node is in good condition
		if node.get_state() != DHTNodeState.GOOD:
			raise RuntimeError(f"{node} is not in good state")

		buckets = self.buckets.copy()
		while buckets:
			for b in buckets:
				# skip wrong buckets
				if not b.is_suitable(node.id):
					continue

				# check is full and split if possible
				if b.is_full():
					if b.is_suitable(self.local_node_id) and b.can_split():
						# split the bucket
						buckets = b.split()
						# remove old bucket
						self.buckets.remove(b)
						# add new buckets to table
						self.buckets.extend(buckets)
					else:
						# TODO: check for bad nodes in bucket and replace
						buckets = []
					break

				# finally add good node to bucket
				b.add_node(node.id)
				# and to map
				self.nodes[node.id] = node
				buckets = []

	def get_closest_nodes(self, target: bytes) -> bytes:
		sorted_nodes = sorted(
			((bytes_to_int(distance(target, node_id)), node) for node_id, node in self.nodes.items()),
			key=lambda x: x[0])[:8]
		nodes: bytearray = bytearray()
		for d, node in sorted_nodes:
			nodes.extend(node.compact_node_info)
		return bytes(nodes)
