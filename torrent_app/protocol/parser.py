from typing import Dict, Any

ROOT = b'root'
DICT = b'd'
LIST = b'l'
END = b'e'
INT = b'i'
EMPTY = b''


class _Block:

	def __init__(self):
		self.parts = []
		self.value = None

	def read(self, data: bytes, position):
		position += 1
		current_byte = data[position].to_bytes(1)
		while current_byte != END:
			child = {DICT: _Dict, LIST: _Array, INT: _Int}.get(current_byte, _Bytes)()
			self.parts.append(child)
			position = child.read(data, position)
			current_byte = data[position].to_bytes(1)

		return position + 1

	def build(self):
		return self.value


class _Bytes(_Block):
	def read(self, data: bytes, position):
		size_len = 0
		current_byte = data[position].to_bytes(1)
		while current_byte != b':':
			size_len += 1
			current_byte = data[position + size_len].to_bytes(1)

		size = int(data[position: position + size_len])
		position += 1 + size_len
		self.value = data[position: position + size]
		# try:
		# 	self.value = tmp.decode("utf-8")
		# except UnicodeDecodeError as err:
		# 	self.value = tmp

		position += size
		return position

	@staticmethod
	def encode(value) -> bytes:
		if isinstance(value, str):
			value = value.encode("utf-8")
		return str(len(value)).encode("utf-8") + b":" + value


class _Int(_Block):
	def read(self, data: bytes, position):
		value = EMPTY
		position += 1
		current_byte = data[position].to_bytes(1)
		while current_byte != END:
			value += current_byte
			position += 1
			current_byte = data[position].to_bytes(1)

		self.value = int(value)

		return position + 1

	@staticmethod
	def encode(value: int) -> bytes:
		return INT + str(value).encode("utf-8") + END


class _Array(_Block):
	def build(self):
		return [part.build() for part in self.parts]

	@staticmethod
	def encode(value) -> bytes:
		result = EMPTY
		for item in value:
			result += encode(item)
		return LIST + result + END


class _Dict(_Block):
	def build(self):
		return {self.parts[i].build().decode("utf-8"): self.parts[i + 1].build() for i in range(0, len(self.parts), 2)}

	@staticmethod
	def encode(value) -> bytes:
		result = EMPTY
		for key, value in sorted(value.items()):
			result += encode(key)
			result += encode(value)
		return DICT + result + END


def decode(data: bytes) -> Dict[str, Any]:
	position = 0
	result = _Dict()
	result.read(data, position)
	return result.build()


def encode(value) -> bytes:
	if isinstance(value, dict):
		return _Dict.encode(value)
	elif isinstance(value, list):
		return _Array.encode(value)
	elif isinstance(value, tuple):
		return _Array.encode(value)
	elif isinstance(value, str):
		return _Bytes.encode(value)
	elif isinstance(value, bytes):
		return _Bytes.encode(value)
	elif isinstance(value, int):
		return _Int.encode(value)
	else:
		raise Exception("unknown type " + str(type(value)))
