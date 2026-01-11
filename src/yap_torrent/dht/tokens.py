import hashlib
import ipaddress
import secrets
import time
from typing import List, Tuple

from yap_torrent.dht.utils import compact_address


class DHTTokens:
	UPDATE_INTERVAL = 60 * 5  # change secret every 5 minutes
	ACTIVE_INTERVAL = 60 * 15  # secret is active up to 15 minutes

	def __init__(self, host: str, port: int) -> None:
		self.__local_ip = compact_address(host, port)
		self.__secrets: List[Tuple[float, bytes]] = []

	def create(self, host: str) -> bytes:
		return hashlib.sha1(ipaddress.ip_address(host).packed + self.__get_secret()).digest()

	def check(self, host: str, token: bytes) -> bool:
		current_time = time.monotonic()
		return any(current_time - t < DHTTokens.ACTIVE_INTERVAL and hashlib.sha1(
			ipaddress.ip_address(host).packed + secret).digest() == token for t, secret in self.__secrets)

	def __get_secret(self) -> bytes:
		current_time = time.monotonic()
		active_secrets = [secret for t, secret in self.__secrets if current_time - t < self.UPDATE_INTERVAL]

		# find secret
		if active_secrets:
			return active_secrets[-1]

		# need new secret
		secret = secrets.token_bytes(20)
		# clean_outdated
		self.__secrets = [(t, secret) for t, secret in self.__secrets if current_time - t < self.ACTIVE_INTERVAL]
		self.__secrets.append((current_time, secret))

		return secret
