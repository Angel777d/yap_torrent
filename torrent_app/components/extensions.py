from typing import Dict, Optional

from angelovichcore.DataStorage import EntityComponent

UT_METADATA = "ut_metadata"
METADATA_PIECE_SIZE = 2 ** 14


# Extension Protocol
class PeerExtensionsEC(EntityComponent):
	EXT_TO_ID: dict[str, int] = {}
	LOCAL_ID_TO_EXT = {}

	def __init__(self, remote_ext_to_id: dict[str, int]):
		super().__init__()
		self.remote_ext_to_id: dict[str, int] = remote_ext_to_id

	@classmethod
	async def setup(cls, *supported: str):
		cls.EXT_TO_ID = {ext: index for index, ext in enumerate(supported, start=1)}
		cls.LOCAL_ID_TO_EXT = {v: k for k, v in cls.EXT_TO_ID.items()}


class TorrentMetadataEC(EntityComponent):
	def __init__(self):
		super().__init__()
		self.metadata_size: int = -1

		self.metadata: bytes = bytes()
		self.pieces: Optional[Dict[int, bytes]] = {}

	def add_piece(self, index: int, piece: bytes):
		self.pieces[index] = piece

	def is_complete(self) -> bool:
		return len(self.metadata) == self.metadata_size

	def set_metadata(self, metadata: bytes) -> "TorrentMetadataEC":
		self.metadata = metadata
		self.metadata_size = len(metadata)
		self.pieces = None
		return self
