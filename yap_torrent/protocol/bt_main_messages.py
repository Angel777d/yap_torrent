import struct
from enum import Enum

from yap_torrent.protocol.message import Message


class MessageId(Enum):
	CHOKE = 0  # <len=0001><id=0>
	UNCHOKE = 1  # <len=0001><id=1>
	INTERESTED = 2  # <len=0001><id=2>
	NOT_INTERESTED = 3  # <len=0001><id=3>
	HAVE = 4  # <len=0005><id=4><piece index>
	BITFIELD = 5  # <len=0001+X><id=5><bitfield>
	REQUEST = 6  # <len=0013><id=6><index><begin><length>
	PIECE = 7  # <len=0009+X><id=7><index><begin><block>
	CANCEL = 8  # <len=0013><id=8><index><begin><length>


for i in MessageId:
	Message.register_name(i.value, i.name)


def payload_index(message: Message) -> int:
	if message.message_id == MessageId.HAVE.value:
		return struct.unpack("!I", message.payload)[0]
	raise RuntimeError("wrong message type for index property")


def payload_bitfield(message: Message) -> bytes:
	if message.message_id == MessageId.BITFIELD.value:
		return message.payload
	raise RuntimeError("wrong message type for bitfield property")


def payload_piece(message: Message) -> tuple[int, int, bytes]:
	if message.message_id == MessageId.PIECE.value:
		payload = message.payload
		return struct.unpack(f"!II{len(payload) - 8}s", payload)
	raise RuntimeError("wrong message type for piece property")


def payload_request(message: Message) -> tuple[int, int, int]:
	if message.message_id == MessageId.REQUEST.value:
		return struct.unpack(f"!III", message.payload)
	raise RuntimeError("wrong message type for request property")


def choke() -> bytes:
	return struct.pack('!B', MessageId.CHOKE.value)


def unchoke() -> bytes:
	return struct.pack('!B', MessageId.UNCHOKE.value)


def interested() -> bytes:
	return struct.pack('!B', MessageId.INTERESTED.value)


def not_interested() -> bytes:
	return struct.pack('!B', MessageId.NOT_INTERESTED.value)


def have(piece_index) -> bytes:
	return struct.pack('!BI', MessageId.HAVE.value, piece_index)


def bitfield(bitfield_value: bytes) -> bytes:
	return struct.pack(f'!B{len(bitfield_value)}s', MessageId.BITFIELD.value, bitfield_value)


def request(piece_index: int, begin: int, length: int) -> bytes:
	return struct.pack('!BIII', MessageId.REQUEST.value, piece_index, begin, length)


def piece(piece_index: int, begin: int, block: bytes) -> bytes:
	return struct.pack(f'!BII{len(block)}s', MessageId.PIECE.value, piece_index, begin, block)


def cancel(piece_index: int, begin: int, length: int) -> bytes:
	return struct.pack('!BIII', MessageId.CANCEL.value, piece_index, begin, length)
