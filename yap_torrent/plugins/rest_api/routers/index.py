from fastapi import APIRouter

router = APIRouter(
	prefix="",
	tags=["root"],
	# dependencies=[Depends(get_token_header)],
	responses={404: {"description": "Not found"}},
)


@router.get("/")
async def read_users():
	return {"Hello": "World"}
