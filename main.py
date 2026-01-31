import os
import httpx
from fastapi import FastAPI, Request, HTTPException

app = FastAPI()

@app.get("/strava/callback")
async def strava_callback(request: Request):
    code = request.query_params.get("code")
    error = request.query_params.get("error")

    if error:
        raise HTTPException(status_code=400, detail="Authorization denied")

    if not code:
        raise HTTPException(status_code=400, detail="Missing code")

    client_id = os.getenv("STRAVA_CLIENT_ID")
    client_secret = os.getenv("STRAVA_CLIENT_SECRET")
    if not client_id or not client_secret:
        raise HTTPException(status_code=500, detail="Missing STRAVA env vars")

    async with httpx.AsyncClient() as client:
        r = await client.post(
            "https://www.strava.com/oauth/token",
            json={
                "client_id": client_id,
                "client_secret": client_secret,
                "code": code,
                "grant_type": "authorization_code",
            },
        )

    if r.status_code >= 400:
        raise HTTPException(status_code=400, detail=r.text)

    return {"status": "authorized", "token_data": r.json()}

# CLI / GitHub Actions only (won't run under uvicorn)
if __name__ == "__main__":
    from extract import main as run_extract
    run_extract()
