import requests

from torrent.structures import TrackerAnnounceResponse


def make_announce(
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
		tracker_id: str = ""
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
		params["trackerid"] = tracker_id

	response = requests.get(
		url=announce,
		params=params,
		headers=headers,
	)

	if response.status_code != 200:
		return None

	return TrackerAnnounceResponse(response.content, compact=compact)
