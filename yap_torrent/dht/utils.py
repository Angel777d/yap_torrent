import ipaddress
import struct


def distance(a: bytes, b: bytes) -> bytes:
	return bytes(x ^ y for x, y in zip(a, b))


def bytes_to_int(value: bytes) -> int:
	return int.from_bytes(value, byteorder='big')


def int_to_bytes(i: int) -> bytes:
	length = i.bit_length() // 8
	return i.to_bytes(length, byteorder='big')


def compact_address(host: str, port: int) -> bytes:
	return ipaddress.ip_address(host).packed + struct.pack('!H', port)


def read_compact_node_info(nodes: bytes):
	i = 0
	while i < len(nodes):
		node_id = nodes[i:i + 20]
		i += 20
		host = ipaddress.ip_address(nodes[i:i + 4])
		i += 4
		port = nodes[i:i + 2]
		i += 2
		yield node_id, host.compressed, int.from_bytes(port, "big")
