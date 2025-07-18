
from fastapi import FastAPI, Request

from src.shard import Shard

# =========================================================================== #

app = FastAPI()

# --------------------------------------------------------------------------- #

@app.get("/process")
async def process_data(request: Request):
    return Shard.process(await request.json())

# =========================================================================== #