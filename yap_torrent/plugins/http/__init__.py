import logging

from angelovich.http import Method
from angelovich.http.server import Server
from yap_torrent import Env
from yap_torrent.plugins import TorrentPlugin
from yap_torrent.plugins.http.torrents import TorrentsHandler


class HttpPlugin(TorrentPlugin):
	def __init__(self):
		self.server = Server()

	async def start(self, env: Env):
		self.server.add_handler(Method.GET, "/torrent", TorrentsHandler(env))
		await self.server.run(host="localhost", port=8080)

	def close(self):
		self.server.stop()


logger = logging.getLogger(__name__)
plugin = HttpPlugin()
logger.info(f"YAP Http plugin imported")
