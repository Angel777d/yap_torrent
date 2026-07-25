import logging
import time
import uuid
from typing import Any, Dict, Optional, Tuple

import aiohttp
from aiohttp import web

from yap_torrent.env import Env

from .mapping import IdManager
from .methods import METHODS, UNIMPLEMENTED

logger = logging.getLogger(__name__)

CSRF_HEADER = "X-Transmission-Session-Id"
DEFAULT_PORT = 9091
DEFAULT_PATH = "/transmission/rpc"


class RpcServer:
	def __init__(self, env: Env):
		self.env = env
		self.ids = IdManager()
		self.session_id = uuid.uuid4().hex + uuid.uuid4().hex[:16]  # 48-char id, like Transmission
		self.start_time = time.monotonic()

		config = env.config.get_plugin_config("yap_torrent_transmission_rpc")
		self.host = config.get("host", "0.0.0.0")
		self.port = int(config.get("port", DEFAULT_PORT))
		self.path = config.get("path", DEFAULT_PATH)

		# Reserved for a future HTTP Basic auth implementation (see _check_auth).
		self._auth_username = config.get("username")
		self._auth_password = config.get("password")

		self.app = self.make_app()
		self.runner = web.AppRunner(self.app)

	# -- lifecycle ---------------------------------------------------------
	def make_app(self) -> web.Application:
		app = web.Application()
		app.router.add_post(self.path, self.handle_rpc)
		return app

	async def start(self):
		await self.runner.setup()
		site = web.TCPSite(self.runner, self.host, self.port)
		await site.start()
		logger.info("Transmission RPC server started on %s:%s%s", self.host, self.port, self.path)

	async def stop(self):
		await self.runner.shutdown()
		await self.runner.cleanup()
		await self.app.cleanup()
		logger.info("Transmission RPC server stopped")

	def close(self):
		pass

	# -- request handling --------------------------------------------------
	async def handle_rpc(self, request: web.Request) -> web.Response:
		# Auth placeholder. Intentionally permissive for now; the 401 path is
		# wired so enabling real auth later is a one-line change in _check_auth.
		if not self._check_auth(request):
			return web.Response(
				status=401,
				headers={"WWW-Authenticate": 'Basic realm="yap_torrent"'},
			)

		# CSRF handshake: a missing/stale session id gets a 409 carrying the
		# current id, which well-behaved clients resend with (rpc-spec).
		if request.headers.get(CSRF_HEADER) != self.session_id:
			return web.Response(
				status=409,
				headers={CSRF_HEADER: self.session_id},
				text=f"{CSRF_HEADER}: {self.session_id}",
			)

		try:
			body = await request.json()
		except Exception:  # noqa: BLE001 - malformed body is a client error
			return web.json_response({"result": "invalid request: malformed json body"})

		method = body.get("method")
		arguments = body.get("arguments") or {}
		tag = body.get("tag")

		result, out_args = await self._dispatch(method, arguments)

		response: Dict[str, Any] = {"result": result, "arguments": out_args}
		if tag is not None:
			response["tag"] = tag
		return web.json_response(response)

	async def _dispatch(self, method: Optional[str], arguments: Dict[str, Any]) -> Tuple[str, Dict[str, Any]]:
		if not method:
			return "no method name given", {}

		handler = METHODS.get(method)
		if handler is not None:
			try:
				return await handler(self, arguments)
			except Exception as ex:  # noqa: BLE001 - never leak a 500 to the client
				logger.exception("RPC method %s failed", method)
				return f"internal error handling {method}: {ex}", {}

		if method in UNIMPLEMENTED:
			return UNIMPLEMENTED[method], {}

		return f"method name not recognized: {method}", {}

	def _check_auth(self, request: web.Request) -> bool:
		# TODO: implement HTTP Basic auth. When self._auth_username /
		#  self._auth_password are set, verify request.headers["Authorization"]
		#  against them and return False on mismatch. Disabled for now.
		return True

	# -- helpers -----------------------------------------------------------
	async def fetch_url(self, url: str) -> Optional[bytes]:
		"""Download a remote .torrent file for torrent-add's filename=URL form."""
		try:
			async with aiohttp.ClientSession() as session:
				async with session.get(url) as resp:
					if resp.status != 200:
						logger.warning("fetch %s returned status %s", url, resp.status)
						return None
					return await resp.read()
		except Exception:  # noqa: BLE001
			logger.exception("failed to fetch torrent url %s", url)
			return None
