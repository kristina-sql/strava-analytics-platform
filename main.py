import os
import httpx
from fastapi import FastAPI, Request, HTTPException
from extract import main as run_extract

app = FastAPI()

@app.get("/strava/callback")
async def strava_callback(request: Request):
    code = request.query_params.get("code")
    error = request.query_params.get("error")

    if error:
        raise HTTPException(status_code=400, detail="Authorization denied")

    if not code:
        raise HTTPException(status_code=400, detail="Missing code")

    async with httpx.AsyncClient() as client:
        r = await client.post(
            "https://www.strava.com/oauth/token",
            json={
                "client_id": os.environ["STRAVA_CLIENT_ID"],
                "client_secret": os.environ["STRAVA_CLIENT_SECRET"],
                "code": code,
                "grant_type": "authorization_code",
            },
        )

    token_data = r.json()

    # TODO: store token_data in Neon
    # access_token, refresh_token, expires_at, athlete.id

    return {"status": "authorized"}

# for GitHub Actions behavior unchanged
if __name__ == "__main__":
    run_extract()
