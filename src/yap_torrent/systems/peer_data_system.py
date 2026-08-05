import logging
import pickle
from pathlib import Path
from typing import List, Tuple

from angelovich.core.System import System

from yap_torrent.components.peer_ec import PeerEC, PeerState
from yap_torrent.env import Env
from yap_torrent.protocol.structures import PeerInfo
from yap_torrent.systems import add_known_peer, get_torrent_entity
from yap_torrent.utils import write_atomic

logger = logging.getLogger(__name__)

# (info_hash, host, port)
PeerRecord = Tuple[bytes, str, int]


class PeerDataSystem(System):
	def __init__(self, env: Env):
		super().__init__(env)
		self.path = Path(env.config.peers_file)

	async def start(self):
		self._load()

	def close(self):
		self._save()
		super().close()

	def _load(self):
		if not self.path.exists():
			return
		try:
			with open(self.path, "rb") as f:
				records: List[PeerRecord] = pickle.load(f)
		except Exception as ex:  # noqa: BLE001
			logger.warning("Failed to load peer store %s: %s", self.path, ex)
			return

		count = 0
		for info_hash, host, port in records:
			if get_torrent_entity(self.env, info_hash) is not None:
				entity = add_known_peer(self.env, info_hash, PeerInfo(host, port))
				entity.get_component(PeerEC).can_reach = True
				count += 1
		logger.info("Loaded %s known peers", count)

	def _save(self):
		records: List[PeerRecord] = []
		for peer_entity in self.env.data_storage.get_collection(PeerEC):
			peer_ec = peer_entity.get_component(PeerEC)
			if peer_ec.state == PeerState.Good and peer_ec.can_reach:
				records.append((peer_ec.info_hash, peer_ec.peer_info.host, peer_ec.peer_info.port))

		try:
			write_atomic(self.path, pickle.dumps(records, pickle.DEFAULT_PROTOCOL))
		except OSError as ex:
			logger.warning("Failed to save peer store %s: %s", self.path, ex)
			return
		logger.info("Saved %s good peers", len(records))
