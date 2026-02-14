import logging
from pathlib import Path
from typing import List, Dict, Any

from aiohttp import web

from yap_torrent.components.torrent_ec import TorrentEC, TorrentStatsEC, TorrentInfoEC
from yap_torrent.env import Env
from yap_torrent.systems import get_torrent_name

logger = logging.getLogger(__name__)


class WebServer:
	def __init__(self, env: Env, host: str = '0.0.0.0', port: int = 8080):
		self.env = env
		self.static_dir = Path(__file__).parent / 'static'

		self.app = web.Application()
		self.runner = web.AppRunner(self.app)

	async def start(self):
		self._setup_routes()

		await self.runner.setup()
		config = self.env.config.get_plugin_config("yap_torrent_web")
		host = config.get("host", "0.0.0.0")
		port = config.get("port", 8080)
		site = web.TCPSite(self.runner, host, port)
		await site.start()

		logger.info(f"Web server started: {site.name}")

	def _setup_routes(self):
		self.app.router.add_static('/static', self.static_dir)

		self.app.router.add_get('/', self.handle_index)

		self.app.router.add_get('/api/status', self.handle_status)
		self.app.router.add_get('/api/torrents', self.handle_torrents)

		self.app.router.add_post('/api/torrent/action', self.handle_torrent_action)
		self.app.router.add_post('/api/magnet/add', self.handle_magnet_add)

	async def handle_index(self, request: web.Request) -> web.Response:
		return web.FileResponse(self.static_dir / 'index.html')

	async def handle_status(self, request: web.Request) -> web.Response:
		return web.json_response(self._get_status())

	async def handle_torrents(self, request: web.Request) -> web.Response:
		return web.json_response(self._get_torrents())

	async def handle_torrent_action(self, request: web.Request) -> web.Response:
		try:
			data = await request.json()
			action = data.get('action')
			torrent_hash = data.get('hash')

			if not action or not torrent_hash:
				return web.json_response(
					{'error': 'Missing action or hash'},
					status=400
				)

			# Dispatch the action event
			self.env.event_bus.dispatch(
				f"request.torrent.{action}",
				bytes.fromhex(torrent_hash)
			)

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

	async def handle_magnet_add(self, request: web.Request) -> web.Response:
		"""Handle adding magnet link"""
		try:
			data = await request.json()
			magnet_link = data.get('magnet')

			if not magnet_link:
				return web.json_response(
					{'error': 'Missing magnet link'},
					status=400
				)

			if not isinstance(magnet_link, str) or not magnet_link.startswith('magnet:'):
				return web.json_response(
					{'error': 'Invalid magnet link format'},
					status=400
				)

			# Dispatch the magnet add event
			self.env.event_bus.dispatch("request.magnet.add", magnet_link)

			return web.json_response({
				'success': True,
				'magnet': magnet_link
			})

		except Exception as e:
			logger.error(f"Error handling magnet add: {e}", exc_info=True)
			return web.json_response(
				{'error': str(e)},
				status=500
			)

	def _get_status(self) -> dict:
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
			complete = .0
			if torrent_entity.has_component(TorrentInfoEC):
				info = torrent_entity.get_component(TorrentInfoEC).info
				complete = info.calculate_downloaded(torrent_entity.get_component(TorrentEC).bitfield.have_num)
			result.append({
				"hash": torrent_entity.get_component(TorrentEC).info_hash.hex(),
				"name": f"{get_torrent_name(torrent_entity)} ({torrent_entity.get_component(TorrentStatsEC).state.name})",
				"complete": complete
			})
		return result

	def close(self):
		# TODO: close server
		logger.info("Web server stopped")
