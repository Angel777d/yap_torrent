from typing import List, Tuple, Optional

from yap_torrent.dht.utils import bytes_to_int


class DHTBucket:
	def __init__(self, k, min_node: int, max_node: int, parent_depth=0):
		self._bucket_capacity = k

		self.min_node: int = min_node
		self.max_node: int = max_node
		self.nodes: List[bytes] = []
		self.depth: int = parent_depth + 1

		self.left_bucket: Optional["DHTBucket"] = None
		self.right_bucket: Optional["DHTBucket"] = None

	def is_suitable(self, node: bytes) -> bool:
		if len(node) == 20:
			return self.min_node <= bytes_to_int(node) < self.max_node
		return False

	def is_full(self) -> bool:
		return len(self.nodes) >= self._bucket_capacity

	def add_node(self, node: bytes) -> None:
		if self.is_full():
			raise RuntimeError("bucket full. can't add node")
		self.nodes.append(node)

	def split(self) -> Tuple["DHTBucket", "DHTBucket"]:
		mid = self.min_node + (self.max_node - self.min_node) // 2
		left = DHTBucket(k=self._bucket_capacity, min_node=self.min_node, max_node=mid, parent_depth=self.depth)
		right = DHTBucket(k=self._bucket_capacity, min_node=mid, max_node=self.max_node, parent_depth=self.depth)
		left.right_bucket = right
		left.left_bucket = self.left_bucket
		right.left_bucket = left
		right.right_bucket = self.right_bucket
		result = (left, right)
		for node in self.nodes:
			for b in result:
				if b.is_suitable(node):
					b.add_node(node)
					break
		return result

	def can_split(self, local_node_id: bytes, shallow_edge: int):
		if self.max_node - self.min_node < self._bucket_capacity * 2:
			return False
		if self.depth < shallow_edge:
			return True
		return self.is_suitable(local_node_id)

	def iter_closest_nodes(self):
		current_nodes = self.nodes.copy()
		left = self.left_bucket
		use_left = bool(left)
		right = self.right_bucket

		while current_nodes:
			yield current_nodes.pop()

			if current_nodes:
				continue

			if use_left:
				current_nodes = left.nodes.copy()
				left = left.left_bucket
				use_left = bool(left) and not right
			elif right:
				current_nodes = right.nodes.copy()
				right = right.right_bucket
				use_left = bool(left)
