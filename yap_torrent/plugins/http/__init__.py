import logging

from angelovich import http
from angelovich.http import Method
from angelovich.http.server import Handler
from yap_torrent import Env
from yap_torrent.plugins import TorrentPlugin
from yap_torrent.plugins.http.torrents import TorrentsHandler


class AppHandler(Handler):
	def __init__(self, env: Env):
		self.env = env


class HttpPlugin(TorrentPlugin):
	def __init__(self):
		self.server = http.Server()

	async def start(self, env: Env):
		self.server.add_handler(Method.GET, "/torrent", TorrentsHandler(env))
		await self.server.run(host="0.0.0.0", port=8080)

	def close(self):
		self.server.stop()


logger = logging.getLogger(__name__)
plugin = HttpPlugin()
logger.info(f"YAP Http plugin imported")
