import asyncio
import logging

from torrent_app import TimeSystem, Env
from torrent_app.components.peer_ec import PeerConnectionEC
from torrent_app.components.piece_ec import PieceEC

logger = logging.getLogger(__name__)


class ProfileSystem(TimeSystem):
	def __init__(self, env: Env):
		super().__init__(env, 15)

	async def _update(self, delta_time: float):
		tasks = asyncio.all_tasks()
		logger.info(f"Task count: {len(tasks)}")

		connections = self.env.data_storage.get_collection(PeerConnectionEC)
		logger.info(f"Alive connections count: {len(connections)}")

		pieces = self.env.data_storage.get_collection(PieceEC)
		logger.info(f"pieces: {len(pieces)}")

		# total, used, free = map(int, os.popen('free -t -m').readlines()[-1].split()[1:])
		# logger.info(f"mem info: total {total} / used {used} / free {free}")
		pass
