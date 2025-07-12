import asyncio
import time
from typing import List

import upnp
from system import Config, Storage, System
from system.torrents import Torrents
from system.watcher import Watcher

GLOBAL_TICK_TIME = 1


def network_setup(port: int) -> tuple[str, str]:
    ip: str = upnp.get_my_ip()
    service = upnp.discover(ip)
    if service:
        open_res = upnp.open_port(service, port, ip)
        print(f"open port: {open_res}")
    return ip, upnp.get_my_ext_ip()

def create_peer_id():
    return b'-PY0001-111111111111'

async def main():
    config = Config()
    _, external_ip = network_setup(config.port)
    storage = Storage(create_peer_id(), external_ip)

    systems: List[System] = [
        await Watcher(config, storage).start(),
        await Torrents(config, storage).start()
    ]

    last_time = time.time()

    while True:

        await asyncio.sleep(GLOBAL_TICK_TIME)
        current_time = time.time()
        dt = current_time - last_time
        last_time = current_time

        for system in systems:
            await system.update(dt)


asyncio.run(main())
