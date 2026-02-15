import logging
from pathlib import Path
from typing import List, Dict, Any

from aiohttp import web

from yap_torrent.components.torrent_ec import TorrentEC, TorrentStatsEC, TorrentInfoEC, TorrentState, TorrentPathEC
from yap_torrent.env import Env
from yap_torrent.systems import get_torrent_name, get_torrent_entity

logger = logging.getLogger(__name__)


class WebServer:
	def __init__(self, env: Env):
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

	async def stop(self):
		await self.runner.shutdown()
		await self.runner.cleanup()
		await self.app.cleanup()
		logger.info("Web server stopped")

	def close(self):
		pass

	def _setup_routes(self):
		self.app.router.add_static('/static', self.static_dir)

		self.app.router.add_get('/', self.handle_index)
		self.app.router.add_get('/torrent/{hash}', self.handle_torrent_page)

		self.app.router.add_get('/api/status', self.handle_status)
		self.app.router.add_get('/api/torrents', self.handle_torrents)
		self.app.router.add_get('/api/torrent/{hash}', self.handle_torrent_info)

		self.app.router.add_post('/api/torrent/action', self.handle_torrent_action)
		self.app.router.add_post('/api/magnet/add', self.handle_magnet_add)

	async def handle_index(self, _: web.Request) -> web.Response:
		return web.FileResponse(self.static_dir / 'index.html')

	async def handle_torrent_page(self, _: web.Request) -> web.Response:
		return web.FileResponse(self.static_dir / 'torrent.html')

	async def handle_status(self, _: web.Request) -> web.Response:
		return web.json_response(self._get_status())

	async def handle_torrents(self, _: web.Request) -> web.Response:
		return web.json_response(self._get_torrents())

	async def handle_torrent_info(self, request: web.Request) -> web.Response:
		return web.json_response(self._get_torrent_info(request.match_info['hash']))

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

	def _get_torrent_info(self, hex_hash: str):
		info_hash = bytes.fromhex(hex_hash)
		torrent_entity = get_torrent_entity(self.env, info_hash)
		if torrent_entity is None:
			raise web.HTTPNotFound(reason="Torrent not found")

		info = torrent_entity.get_component(TorrentInfoEC).info
		stats = torrent_entity.get_component(TorrentStatsEC)

		return {
			"hash": hex_hash,
			"files": sorted(["/".join(s.decode("utf-8") for s in file.path) for file in info.files]),
			"name": get_torrent_name(torrent_entity),
			"isStarted": stats.state == TorrentState.Active,
			"complete": info.calculate_downloaded(torrent_entity.get_component(TorrentEC).bitfield.have_num),
			"uploaded": stats.uploaded,
			"downloaded": stats.downloaded,
			"downloadPath": torrent_entity.get_component(TorrentPathEC).root_path.absolute().as_posix()
		}

	async def handle_torrent_action(self, request: web.Request) -> web.Response:
		data = await request.json()
		action = data.get('action')
		torrent_hash = data.get('hash')

		if not action or not torrent_hash:
			raise web.HTTPBadRequest(reason="Missing action or hash")

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

	async def handle_magnet_add(self, request: web.Request) -> web.Response:
		data = await request.json()
		magnet_link = data.get('magnet')

		if not magnet_link:
			raise web.HTTPBadRequest(reason="Missing magnet link")

		if not isinstance(magnet_link, str) or not magnet_link.startswith('magnet:'):
			raise web.HTTPBadRequest(reason="Invalid magnet link format")

		# Dispatch the magnet add event
		self.env.event_bus.dispatch("request.magnet.add", magnet_link)

		return web.json_response({
			'success': True,
			'magnet': magnet_link
		})
