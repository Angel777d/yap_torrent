from typing import Dict

from angelovich.core.DataStorage import EntityComponent

UT_METADATA = "ut_metadata"
METADATA_PIECE_SIZE = 2 ** 14


# Extension Protocol
class PeerExtensionsEC(EntityComponent):
	EXT_TO_ID: dict[str, int] = {}
	__ID_TO_EXT: dict[int, str] = {}

	@classmethod
	def add_supported(cls, supported_extension: str):
		cls.EXT_TO_ID[supported_extension] = len(cls.EXT_TO_ID) + 1  # Start from index 1
		cls.__ID_TO_EXT = {v: k for k, v in cls.EXT_TO_ID.items()}

	def __init__(self, remote_ext_to_id: dict[str, int]):
		super().__init__()
		self.remote_ext_to_id: dict[str, int] = remote_ext_to_id

	def get_extension_name(self, ext_id: int) -> str:
		return self.__ID_TO_EXT.get(ext_id, "")


class TorrentMetadataEC(EntityComponent):
	def __init__(self):
		super().__init__()
		self.metadata_size: int = -1

		self.metadata: bytes = bytes()
		self.pieces: Dict[int, bytes] = {}

	def add_piece(self, index: int, piece: bytes):
		self.pieces[index] = piece

	def is_complete(self) -> bool:
		return len(self.metadata) == self.metadata_size

	def set_metadata(self, metadata: bytes) -> "TorrentMetadataEC":
		self.metadata = metadata
		self.metadata_size = len(metadata)
		self.pieces.clear()
		return self
