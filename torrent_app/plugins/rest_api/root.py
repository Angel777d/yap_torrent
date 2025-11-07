from fastapi import FastAPI

from torrent_app.plugins.rest_api.routers import torrents

app = FastAPI()
app.include_router(torrents.router)


@app.get("/")
def read_root():
	return {"Hello": "World"}
