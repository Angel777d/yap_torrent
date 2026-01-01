import logging
from typing import List, Dict, Tuple, Iterable

from yap_torrent.dht.routing.bucket import DHTBucket
from yap_torrent.dht.routing.nodes import DHTNode, DHTNodeState

logger = logging.getLogger(__name__)

FULL_NODES_RANGE = 2 ** 160
SHALLOW_EDGE = 5


class DHTRoutingTable:
	def __init__(self, local_node_id: bytes, k, max_range=FULL_NODES_RANGE, shallow_edge=SHALLOW_EDGE):
		self._shallow_edge = shallow_edge
		self.local_node_id = local_node_id

		self.buckets: List[DHTBucket] = [DHTBucket(k=k, min_node=0, max_node=max_range)]
		self.nodes: Dict[bytes, DHTNode] = {}

	def _find_bucket(self, node_id: bytes) -> DHTBucket:
		for bucket in self.buckets:
			if bucket.is_suitable(node_id):
				return bucket
		logger.debug('There is no bucket for node %s.', node_id)
		return self.buckets[0]

	def _cleanup(self, bucket: DHTBucket):
		for node_id in bucket.nodes:
			if self.nodes[node_id].get_state() == DHTNodeState.BAD:
				bucket.nodes.remove(node_id)
				del self.nodes[node_id]

	def touch(self, node_id: bytes, host: str, port: int) -> bool:
		if node_id in self.nodes:
			self.nodes[node_id].mark_good()
			# self._find_bucket(node_id).touch()
			return True
		return self.add_node(node_id, host, port)

	def add_node(self, node_id: bytes, host: str, port: int) -> bool:
		node = DHTNode(node_id, host, port)
		node.mark_good()
		bucket = self._find_bucket(node_id)

		# try to clean up bad nodes
		self._cleanup(bucket)

		buckets: Iterable[DHTBucket] = [bucket]
		while buckets:
			for bucket in buckets:
				# skip wrong bucket
				if not bucket.is_suitable(node.id):
					continue

				# check is full and split if possible
				if bucket.is_full():
					if bucket.can_split(self.local_node_id, self._shallow_edge):
						# split the bucket
						buckets = bucket.split()
						# replace old bucket with new
						self.buckets.remove(bucket)
						self.buckets.extend(buckets)
					else:
						# There is no place for the new node
						return False
					break

				# finally, add good node to bucket
				bucket.add_node(node.id)
				# and to map
				self.nodes[node.id] = node
				buckets = []

		logger.debug('Add %s to routing table', node)
		return True

	def get_closest_nodes(self, target_node: bytes, k: int) -> List[DHTNode]:
		return list(
			self.nodes[node_id] for _, node_id in zip(range(k), self._find_bucket(target_node).iter_closest_nodes()))

	def export_nodes(self) -> List[Tuple[bytes, str, int]]:
		def iter_nodes():
			for b in self.buckets:
				for node_id in b.nodes:
					node = self.nodes[node_id]
					if node.get_state() != DHTNodeState.BAD:
						yield node.id, node.host, node.port

		return list(iter_nodes())
