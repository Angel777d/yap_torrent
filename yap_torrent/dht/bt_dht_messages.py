import struct

from yap_torrent.protocol.message import Message

PORT = 9  # <len=0003><id=9><port>
Message.register_name(PORT, "PORT")


def payload_port(message: Message) -> int:
	if message.message_id == PORT:
		return struct.unpack(f"!H", message.payload)[0]
	raise RuntimeError("wrong message type for 'port' property")


def port(port_value: int) -> bytes:
	return struct.pack('!BH', PORT, port_value)
