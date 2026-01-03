from angelovich.http import Protocol, Code
from angelovich.http.request import HTTPRequest
from angelovich.http.response import HTTPResponse
from angelovich.http.server import Handler
from yap_torrent import Env

from yap_torrent.components.torrent_ec import TorrentHashEC, TorrentInfoEC


class AppHandler(Handler):
	def __init__(self, env: Env):
		self.env = env

class TorrentsHandler(AppHandler):
	async def on_request(self, path: str, request: HTTPRequest) -> HTTPResponse:
		torrents = self.env.data_storage.get_collection(TorrentHashEC).entities
		headers = ["Content-Type: text/plain; charset=UTF-8"]

		body: str = ""
		for torrent in torrents:
			if torrent.has_component(TorrentInfoEC):
				body += torrent.get_component(TorrentInfoEC).info.name

			info_hash = torrent.get_component(TorrentHashEC).info_hash
			body += f"[{info_hash.hex()}]\r\n"

		return HTTPResponse(Protocol.HTTP1_1, Code.OK, headers, body.encode("utf-8"))
