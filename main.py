
from fastapi import FastAPI, Request

from src.shard import Shard

# =========================================================================== #

app = FastAPI()

# --------------------------------------------------------------------------- #

@app.get("/")
def read_root():
    return {"message": "Welcome to the NWP Forecast Integrate API"}

# --------------------------------------------------------------------------- #

@app.post("/process")
async def process_data(request: Request):
    return Shard.process(await request.json())

# =========================================================================== #