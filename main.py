import os
import httpx
from fastapi import FastAPI, Request
from fastapi.responses import PlainTextResponse

app = FastAPI()

@app.get("/strava/callback")
async def strava_callback(request: Request):
    return request.query_params

