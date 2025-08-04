from app.config import Config
from core.DataStorage import DataStorage


class Env:
    def __init__(self, peer_id: bytes, external_ip: str, cfg: Config):
        self.peer_id: bytes = peer_id
        self.external_ip: str = external_ip
        self.config: Config = cfg
        self.data_storage = DataStorage()


class System:
    def __init__(self, env: Env):
        self.env: Env = env

    async def start(self) -> 'System':
        return self

    async def update(self, delta_time: float):
        pass
