from fastapi import APIRouter

from torrent_app.components.torrent_ec import TorrentHashEC
from torrent_app.plugins.rest_api.global_state import get_env

router = APIRouter(
	prefix="/torrents",
	tags=["torrents"],
	# dependencies=[Depends(get_token_header)],
	responses={404: {"description": "Not found"}},
)


@router.get("/")
async def read_users():
	ds = get_env().data_storage
	return [{"hash_info": str(e.get_component(TorrentHashEC).info_hash)} for e in
	        ds.get_collection(TorrentHashEC).entities]


@router.get("/{hash_info}")
async def read_user(hash_info: str):
	return {"hash_info": hash_info}
