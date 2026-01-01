import struct
from typing import Any, Tuple, Dict

from yap_torrent.protocol import decode
from yap_torrent.protocol.message import Message

EXTENDED = 20  # <len=0001+X><id=20><extended message ID>
Message.register_name(EXTENDED, "EXTENDED")


def payload_extended(message: Message) -> Tuple[int, Dict[str, Any]]:
	payload = message.payload
	return payload[0], decode(payload[1:])


def extended(ext_id: int, payload: bytes) -> bytes:
	return struct.pack(f'!BB{len(payload)}s', EXTENDED, ext_id, payload)
