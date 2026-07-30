import logging
import time
import uuid
from typing import Any, Dict, Optional, Tuple

from aiohttp import web

from yap_torrent.env import Env
from .components import PLUGIN_CONFIG_KEY, get_speed_settings, get_torrent_ids
from .methods import CORE_SETTINGS, METHODS, UNIMPLEMENTED, ServerInfo

logger = logging.getLogger(__name__)

CSRF_HEADER = "X-Transmission-Session-Id"
DEFAULT_PORT = 9091
DEFAULT_PATH = "/transmission/rpc"


class RpcServer:
	def __init__(self, env: Env):
		self.env = env

		config = env.config.get_plugin_config(PLUGIN_CONFIG_KEY)

		self.info: ServerInfo = ServerInfo(
			uuid.uuid4().hex + uuid.uuid4().hex[:16],  # 48-char id, like Transmission
			time.monotonic(),
		)
		# seed the app-wide speed singleton from config now, so its initial values come
		# from the file rather than from whichever request happens to touch it first
		get_speed_settings(env)
		# a removed torrent's session id is dead weight; nothing may reuse the number
		env.event_bus.add_listener("action.torrent.remove", self._on_torrent_remove, scope=self)
		# which config properties this RPC lets a client change, and how to read each
		env.settings.register(*CORE_SETTINGS)

		self.host = config.get("host", "0.0.0.0")
		self.port = int(config.get("port", DEFAULT_PORT))
		self.path = config.get("path", DEFAULT_PATH)

		# Reserved for a future HTTP Basic auth implementation (see _check_auth).
		self._auth_username = config.get("username")
		self._auth_password = config.get("password")

		self.app = self.make_app()
		self.runner = web.AppRunner(self.app, access_log=None)

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
		return self

	async def _on_torrent_remove(self, info_hash: bytes):
		get_torrent_ids(self.env).forget(info_hash)

	async def stop(self):
		self.env.event_bus.remove_all_listeners(scope=self)
		await self.runner.shutdown()
		await self.runner.cleanup()
		await self.app.cleanup()
		logger.info("Transmission RPC server stopped")

	def close(self):
		pass

	# -- request handling --------------------------------------------------
	async def handle_rpc(self, request: web.Request) -> web.Response:
		if not self._check_auth(request):
			return web.Response(
				status=401,
				headers={"WWW-Authenticate": 'Basic realm="yap_torrent"'},
			)

		# CSRF handshake: a missing/stale session id gets a 409 carrying the
		# current id, which well-behaved clients resend with (rpc-spec).
		if request.headers.get(CSRF_HEADER) != self.info.session_id:
			return web.Response(
				status=409,
				headers={CSRF_HEADER: self.info.session_id},
				text=f"{CSRF_HEADER}: {self.info.session_id}",
			)

		try:
			body = await request.json()
		except Exception as ex:
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
				return await handler(self.env, self.info, arguments)
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
