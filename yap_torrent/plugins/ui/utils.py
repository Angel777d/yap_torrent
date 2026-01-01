from typing import Optional

from angelovichcore.DataStorage import Entity
from yap_torrent.components.torrent_ec import TorrentInfoEC, TorrentHashEC


def get_torrent_name(entity: Optional[Entity], default_text="No Data") -> str:
	if entity:
		if entity.has_component(TorrentInfoEC):
			return entity.get_component(TorrentInfoEC).info.name
		else:
			return f"[{entity.get_component(TorrentHashEC).info_hash}]"
	else:
		return default_text
