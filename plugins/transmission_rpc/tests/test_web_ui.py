"""The bundled web UI, as served by RpcServer.

Only the routing and the one piece of server-side rendering are covered here — the page
itself is a plain RPC client, and everything it can ask for is already exercised by
test_rpc.py through the same endpoint the browser posts to.

Run with:  pytest plugins/transmission_rpc
"""
import pytest

from yap_torrent.config import Config
from yap_torrent.env import Env
from yap_torrent_transmission_rpc.components import PLUGIN_CONFIG_KEY
from yap_torrent_transmission_rpc.server import DEFAULT_WEB_PATH, RPC_PATH_PLACEHOLDER, RpcServer


def make_server(**plugin_config) -> RpcServer:
	# Config() with a missing path falls back to defaults and never writes, so these
	# do not depend on — or disturb — the repo's config.json
	config = Config(path="__no_such_config__.json")
	if plugin_config:
		config.set_plugin_config(PLUGIN_CONFIG_KEY, plugin_config)
	return RpcServer(Env(b"-PY0001-111111111111", "127.0.0.1", "127.0.0.1", config))


@pytest.fixture
async def ui_client(aiohttp_client):
	return await aiohttp_client(make_server().app)


async def test_the_index_is_served_under_the_web_path(ui_client):
	response = await ui_client.get(f"{DEFAULT_WEB_PATH}/")
	assert response.status == 200
	assert response.content_type == "text/html"
	assert "YAP Torrent" in await response.text()


async def test_the_page_is_told_where_the_rpc_lives(ui_client):
	# the endpoint is configurable, so the placeholder must be gone by the time it is served
	body = await (await ui_client.get(f"{DEFAULT_WEB_PATH}/")).text()
	assert RPC_PATH_PLACEHOLDER not in body
	assert '<meta name="rpc-path" content="/transmission/rpc">' in body


async def test_a_custom_rpc_path_reaches_the_page(aiohttp_client):
	client = await aiohttp_client(make_server(path="/my/rpc").app)
	body = await (await client.get(f"{DEFAULT_WEB_PATH}/")).text()
	assert '<meta name="rpc-path" content="/my/rpc">' in body


async def test_the_root_redirects_to_the_ui(ui_client):
	# a browser pointed at the host lands on the client, the way Transmission's own does
	response = await ui_client.get("/", allow_redirects=False)
	assert response.status == 302
	assert response.headers["Location"] == f"{DEFAULT_WEB_PATH}/"


async def test_the_prefix_without_a_slash_redirects_to_the_one_with(ui_client):
	# the index links its assets relatively, so they only resolve under the trailing slash
	response = await ui_client.get(DEFAULT_WEB_PATH, allow_redirects=False)
	assert response.status == 302
	assert response.headers["Location"] == f"{DEFAULT_WEB_PATH}/"


@pytest.mark.parametrize("asset", ["app.js", "style.css"])
async def test_static_assets_are_served(ui_client, asset):
	response = await ui_client.get(f"{DEFAULT_WEB_PATH}/static/{asset}")
	assert response.status == 200
	assert await response.text()


async def test_the_ui_can_be_switched_off_without_touching_the_rpc(aiohttp_client):
	client = await aiohttp_client(make_server(web_enabled=False).app)

	assert (await client.get(f"{DEFAULT_WEB_PATH}/")).status == 404
	assert (await client.get("/", allow_redirects=False)).status == 404
	# the RPC is still there: no session id, so the CSRF handshake answers
	assert (await client.post("/transmission/rpc", json={"method": "session-get"})).status == 409


async def test_the_web_path_is_configurable(aiohttp_client):
	client = await aiohttp_client(make_server(web_path="/ui").app)

	assert (await client.get("/ui/")).status == 200
	assert (await client.get("/ui/static/app.js")).status == 200
	assert (await client.get(f"{DEFAULT_WEB_PATH}/")).status == 404


async def test_a_trailing_slash_in_the_configured_path_does_not_double_up(aiohttp_client):
	# "/ui/" and "/ui" have to name the same UI, or the index 404s under one of them
	client = await aiohttp_client(make_server(web_path="/ui/").app)

	assert (await client.get("/ui/")).status == 200
	assert (await client.get("/ui", allow_redirects=False)).headers["Location"] == "/ui/"
