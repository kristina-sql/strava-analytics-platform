import os
import httpx
from fastapi import FastAPI, Request
from fastapi.responses import PlainTextResponse

app = FastAPI()

@app.get("/strava/callback")
async def strava_callback(request: Request):
    code = request.query_params.get("code")
    if not code:
        return PlainTextResponse("Authorization failed.", status_code=400)

    async with httpx.AsyncClient() as client:
        await client.post(
            "https://www.strava.com/oauth/token",
            json={
                "client_id": os.getenv("STRAVA_CLIENT_ID"),
                "client_secret": os.getenv("STRAVA_CLIENT_SECRET"),
                "code": code,
                "grant_type": "authorization_code",
            },
        )

    return PlainTextResponse("Authorization successful. You can close this tab.")
