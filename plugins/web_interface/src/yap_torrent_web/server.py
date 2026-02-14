import logging
from pathlib import Path
from typing import Optional, List, Dict, Any

from aiohttp import web

from yap_torrent.components.torrent_ec import TorrentEC, TorrentStatsEC
from yap_torrent.env import Env
from yap_torrent.systems import get_torrent_name

logger = logging.getLogger(__name__)


class WebServer:
	def __init__(self, env: Env, host: str = '0.0.0.0', port: int = 8080):
		self.env = env
		self.host = host
		self.port = port
		self.app: Optional[web.Application] = None
		self.runner: Optional[web.AppRunner] = None
		self.site: Optional[web.TCPSite] = None
		self.static_dir = Path(__file__).parent / 'static'

	async def start(self):
		"""Start the web server"""
		self.app = web.Application()
		self._setup_routes()

		self.runner = web.AppRunner(self.app)
		await self.runner.setup()

		self.site = web.TCPSite(self.runner, self.host, self.port)
		await self.site.start()

		logger.info(f"Web server started on http://{self.host}:{self.port}")

	def _setup_routes(self):
		"""Setup application routes"""
		self.app.router.add_get('/', self.handle_index)
		self.app.router.add_get('/index.html', self.handle_index)
		self.app.router.add_get('/api/status', self.handle_status)
		self.app.router.add_get('/api/torrents', self.handle_torrents)
		self.app.router.add_post('/api/torrent/action', self.handle_torrent_action)
		self.app.router.add_static('/static', self.static_dir)

	async def handle_index(self, request: web.Request) -> web.Response:
		return web.FileResponse(self.static_dir / 'index.html')

	async def handle_status(self, request: web.Request) -> web.Response:
		return web.json_response(self._get_status())

	async def handle_torrents(self, request: web.Request) -> web.Response:
		return web.json_response(self._get_torrents())

	async def handle_torrent_action(self, request: web.Request) -> web.Response:
		"""Handle torrent action (start/stop/invalidate/remove/dht_ask_peers)"""
		try:
			data = await request.json()
			action = data.get('action')
			torrent_hash = data.get('hash')

			if not action or not torrent_hash:
				return web.json_response(
					{'error': 'Missing action or hash'},
					status=400
				)

			allowed_actions = ['start', 'stop', 'invalidate', 'remove', 'dht_ask_peers']
			if action not in allowed_actions:
				return web.json_response(
					{'error': f'Invalid action: "{action}". Must be one of: {", ".join(allowed_actions)}'},
					status=400
				)

			# Dispatch the action event
			event_name = f"request.torrent.{action}"
			self.env.event_bus.dispatch(event_name, bytes.fromhex(torrent_hash))

			return web.json_response({
				'success': True,
				'action': action,
				'hash': torrent_hash
			})

		except Exception as e:
			logger.error(f"Error handling torrent action: {e}", exc_info=True)
			return web.json_response(
				{'error': str(e)},
				status=500
			)

	def _get_status(self) -> dict:
		"""Get current status information"""
		return {
			"status": "running",
			"peer_id": self.env.peer_id.hex(),
			"ip": self.env.ip,
			"external_ip": self.env.external_ip,
		}

	def _get_torrents(self) -> List[Dict[str, Any]]:
		ds = self.env.data_storage
		collection = ds.get_collection(TorrentEC)
		result = []
		for index, torrent_entity in enumerate(collection):
			result.append({
				"hash": torrent_entity.get_component(TorrentEC).info_hash.hex(),
				"name": f"{get_torrent_name(torrent_entity)} ({torrent_entity.get_component(TorrentStatsEC).state.name})"}
			)
		return result

	def close(self):
		# TODO: close server
		logger.info("Web server stopped")
