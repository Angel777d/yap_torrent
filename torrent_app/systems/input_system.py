import asyncio
import logging

from torrent_app import System
from torrent_app.components.bitfield_ec import BitfieldEC
from torrent_app.components.peer_ec import KnownPeersEC
from torrent_app.components.torrent_ec import TorrentHashEC
from torrent_app.components.tracker_ec import TorrentTrackerDataEC
from torrent_app.protocol.magnet import MagnetInfo

logger = logging.getLogger(__name__)


# stupid idea to use console input like this
class InputSystem(System):
	async def start(self) -> 'System':
		self.add_task(asyncio.create_task(self.process_input()))
		return await super().start()

	async def process_input(self) -> bytes:
		loop = asyncio.get_running_loop()
		while True:
			await loop.run_in_executor(None, self.wait_input)

	def wait_input(self):
		result = input("add magnet:")
		magnet = MagnetInfo(result)

		entity = self.env.data_storage.create_entity()
		entity.add_component(TorrentHashEC(magnet.info_hash))
		entity.add_component(KnownPeersEC())
		entity.add_component(BitfieldEC())


		if magnet.trackers:
			entity.add_component(TorrentTrackerDataEC([magnet.trackers]))

		# TODO: download first
		# entity.add_component(TorrentInfoEC(torrent_info))

		# TODO: support empty TorrentInfo
		# entity.add_component(TorrentSaveEC())
		logger.info(f"add torrent by magnet: {magnet}")
