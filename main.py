import asyncio

import upnp
from app import Env
from app.application import Application
from app.config import Config


def network_setup(port: int) -> tuple[str, str]:
	ip: str = upnp.get_my_ip()
	service = upnp.discover(ip)
	if service:
		open_res = upnp.open_port(service, port, ip)
		print(f"open port: {open_res}")
	return ip, upnp.get_my_ext_ip()


def create_peer_id():
	return b'-PY0001-111111111111'


def start():
	config = Config()
	ip, external_ip = network_setup(config.port)
	env = Env(create_peer_id(), ip, external_ip, config)

	application = Application(env)
	try:
		asyncio.run(application.run())
	except KeyboardInterrupt:
		application.close()


if __name__ == '__main__':
	start()
