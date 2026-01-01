from yap_torrent import System
from yap_torrent.components.torrent_ec import TorrentHashEC, ValidateTorrentEC


class TorrentSystem(System):

	async def start(self):
		self.env.event_bus.add_listener("request.torrent.invalidate", self._on_torrent_invalidate, scope=self)

		self.env.event_bus.add_listener("request.torrent.start", self._on_torrent_start, scope=self)
		self.env.event_bus.add_listener("request.torrent.stop", self._on_torrent_stop, scope=self)
		self.env.event_bus.add_listener("request.torrent.remove", self._on_torrent_remove, scope=self)

	def close(self) -> None:
		super().close()
		self.env.event_bus.remove_all_listeners(scope=self)

	async def _update(self, delta_time: float):
		pass

	async def _on_torrent_start(self, info_hash: bytes):
		pass

	async def _on_torrent_stop(self, info_hash: bytes):
		pass

	async def _on_torrent_remove(self, info_hash: bytes):
		pass

	async def _on_torrent_invalidate(self, info_hash: bytes):
		torrent_entity = self.env.data_storage.get_collection(TorrentHashEC).find(info_hash)
		torrent_entity.add_component(ValidateTorrentEC())
