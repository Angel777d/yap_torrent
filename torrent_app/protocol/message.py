class Message:
	__NAMES: dict[int, str] = {}

	def __init__(self, buffer: bytes) -> None:
		self.__buffer: bytes = buffer

	@property
	def message_id(self) -> int:
		return self.__buffer[0]

	@property
	def payload(self) -> bytes:
		return self.__buffer[1:]

	@classmethod
	def register_name(cls, message_id: int, name: str) -> None:
		cls.__NAMES[message_id] = name

	def __repr__(self):
		return self.__str__()

	def __str__(self):
		return self.__NAMES.get(self.message_id, f"Message {self.message_id} with no name")
