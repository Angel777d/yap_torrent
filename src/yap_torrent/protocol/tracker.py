import logging

import aiohttp

from yap_torrent.protocol import decode
from yap_torrent.protocol.structures import TrackerAnnounceResponse

logger = logging.getLogger(__name__)


async def make_announce(
		announce: str,
		info_hash: bytes,
		peer_id: bytes,
		downloaded: int = 0,
		uploaded: int = 0,
		left: int = 0,
		ip: str = "127.0.0.1",
		port=6881,
		compact=1,
		event="",
		tracker_id: bytes = b'',
		timeout=10.
) -> TrackerAnnounceResponse | None:
	# peer_id = '-PC0100-123469398945'
	# peer_id = '-qB4230-414563428945'

	headers = {
		"User-Agent": "Transmission/4.1.0",
		"X-Forwarded-For": ip,
	}

	params = {
		'info_hash': info_hash,
		'peer_id': peer_id,
		'port': port,
		'ip': ip,

		'uploaded': uploaded,
		'downloaded': downloaded,
		'left': left,

		'event': event,
		'compact': compact,

		'numwant': 50,
		'corrupt': 0,
		'supportcrypto': 1,
		'redundant': 0
	}

	if tracker_id:
		# TODO: check if bytes ok here
		params["trackerid"] = tracker_id

	try:

		# aiohttp default encoding breaks info_hash bytes
		# made a workaround with requests usage
		def construct_url() -> str:
			from requests import PreparedRequest
			p = PreparedRequest()
			p.prepare(
				method="GET",
				url=announce,
				headers=headers,
				params=params,
			)
			return p.url

		async with aiohttp.ClientSession() as session:
			async with session.request("GET",
			                           url=construct_url(),
			                           # params=params,
			                           headers=headers,
			                           timeout=aiohttp.ClientTimeout(connect=timeout)
			                           ) as response:
				if response.status != 200:
					return None
				data = await response.read()
				return TrackerAnnounceResponse(decode(data), compact)
	except ConnectionError as ex:
		logger.warning(f"Connection error on announce: {ex}")
	except Exception as ex:
		logger.error(f"Unexpected net exception: {ex}")

	return None
