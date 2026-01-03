from angelovich.http import Protocol, Code, Request, Response

from yap_torrent.components.torrent_ec import TorrentHashEC
from yap_torrent.plugins.http import AppHandler


class TorrentsHandler(AppHandler):
	async def on_request(self, path: str, request: Request) -> Response:
		torrents = self.env.data_storage.get_collection(TorrentHashEC).entities

		body: bytes
		return Response(Protocol.HTTP1_1, Code.OK, [], b"")
