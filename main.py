
from fastapi import FastAPI, Request

from src.controller import Controller

# =========================================================================== #

app = FastAPI()

# --------------------------------------------------------------------------- #

@app.get("/process")
async def process_data(request: Request):
    data = await request.json()
    return {"message": "Data processed successfully", "data": data}

# =========================================================================== #