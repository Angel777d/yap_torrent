import ipaddress
from typing import Optional, Any

from yap_torrent.protocol import encode

DHT: bytes = b'\0\0\0\0\0\0\0\x01'  # https://bittorrent.org/beps/bep_0005.html
EXTENSION_PROTOCOL: bytes = b'\0\0\0\0\0\x10\0\0'  # https://www.bittorrent.org/beps/bep_0010.html


def create_reserved(*flags: bytes) -> bytes:
	reserved = bytearray(8)
	for flag in flags:
		for i, byte in enumerate(flag):
			reserved[i] |= byte
	return bytes(reserved)


def merge_reserved(local: bytes, remote: bytes) -> bytes:
	assert len(local) == 8 and len(remote) == 8
	return bytes(a & b for a, b in zip(local, remote))


def check_extension(reserved: bytes, extension: bytes) -> bool:
	return any(a & b for a, b in zip(reserved, extension, strict=True))


def __ip(ip: str | bytes) -> bytes:
	if isinstance(ip, str):
		# TODO: support ipv6
		return bytes(map(int, ip.split('.')))
	return ip


def extension_handshake(
		m: dict,
		p: Optional[int] = None,
		v: Optional[str] = None,
		yourip: Optional[str | bytes] = None,
		ipv6: Optional[str | bytes] = None,
		ipv4: Optional[str | bytes] = None,
		reqq: Optional[int] = None,
		**kwargs,
) -> bytes:
	value: dict[str, Any] = {"m": m}
	if p:
		value["p"] = p
	if v:
		value["v"] = v
	if reqq:
		value["reqq"] = reqq
	if yourip:
		value["yourip"] = ipaddress.IPv4Address(yourip).packed
	if ipv6:
		value["ipv6"] = ipaddress.IPv6Address(ipv6).packed
	if ipv4:
		value["ipv4"] = ipaddress.IPv4Address(ipv4).packed

	value.update(kwargs)

	return encode(value)
