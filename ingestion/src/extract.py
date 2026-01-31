# AI refactored code, need to validate
import json
import os
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Dict, List

import requests

import psycopg2
from psycopg2.extras import execute_values


# -----------------------------
# Small helpers
# -----------------------------

def utc_now_iso() -> str:
    """UTC time in ISO-ish string; good for logging and extracted_at."""
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def require_env(name: str) -> str:
    """Fail fast if env var not set."""
    value = os.getenv(name)
    if not value:
        raise RuntimeError(f"Missing env var: {name}")
    return value


@dataclass
class TokenRow:
    athlete_id: int
    access_token: str
    refresh_token: str
    expires_at: int  # unix epoch seconds


# -----------------------------
# Strava API
# -----------------------------

def refresh_access_token(client_id: str, client_secret: str, refresh_token: str) -> Dict[str, Any]:
    """
    Strava refresh flow.
    NOTE: Strava may rotate refresh tokens, so store the returned refresh_token.
    """
    resp = requests.post(
        "https://www.strava.com/oauth/token",
        data={
            "client_id": client_id,
            "client_secret": client_secret,
            "grant_type": "refresh_token",
            "refresh_token": refresh_token,
        },
        timeout=30,
    )
    resp.raise_for_status()
    return resp.json()


def fetch_activities(access_token: str, per_page: int = 200) -> List[Dict[str, Any]]:
    """
    Fetch all activities for one athlete using paging.
    """
    url = "https://www.strava.com/api/v3/athlete/activities"
    headers = {"Authorization": f"Bearer {access_token}"}

    all_activities: List[Dict[str, Any]] = []
    page = 1

    while True:
        resp = requests.get(
            url,
            headers=headers,
            params={"per_page": per_page, "page": page},
            timeout=30,
        )

        if resp.status_code != 200:
            raise RuntimeError(f"Strava API error {resp.status_code}: {resp.text}")

        data = resp.json()
        if not data:
            break

        all_activities.extend(data)
        page += 1

    return all_activities


# -----------------------------
# Neon (Postgres) read/write
# -----------------------------

def ensure_raw_table(cur) -> None:
    """
    Raw table stores the original Strava payload per activity.
    We include athlete_id because you have multiple athletes.
    """
    cur.execute("create schema if not exists raw;")
    cur.execute(
        """
        create table if not exists raw.strava_activities (
          athlete_id bigint not null,
          activity_id bigint not null,
          extracted_at_utc timestamptz not null,
          payload jsonb not null,
          primary key (athlete_id, activity_id)
        );
        """
    )


def fetch_all_tokens(cur) -> List[TokenRow]:
    """
    Read tokens from your strava_token table.
    Adjust column names if yours are different.
    """
    cur.execute(
        """
        select athlete_id, access_token, refresh_token, expires_at
        from public.strava_tokens
        where refresh_token is not null
        """
    )
    rows = cur.fetchall()
    return [
        TokenRow(
            athlete_id=int(r[0]),
            access_token=str(r[1] or ""),
            refresh_token=str(r[2]),
            expires_at=int(r[3] or 0),
        )
        for r in rows
    ]


def upsert_token(cur, athlete_id: int, access_token: str, refresh_token: str, expires_at: int) -> None:
    """
    Write refreshed tokens back into strava_token.
    Requires athlete_id to be unique / primary key.
    """
    cur.execute(
        """
        insert into public.strava_tokens (athlete_id, access_token, refresh_token, expires_at, updated_at)
        values (%s, %s, %s, %s, now())
        on conflict (athlete_id) do update
          set access_token = excluded.access_token,
              refresh_token = excluded.refresh_token,
              expires_at = excluded.expires_at,
              updated_at = now()
        """,
        (athlete_id, access_token, refresh_token, expires_at),
    )


def upsert_activities(cur, athlete_id: int, extracted_at_utc: str, activities: List[Dict[str, Any]]) -> int:
    """
    Bulk upsert activities into raw.strava_activities.
    execute_values is much faster than looping cur.execute() per activity.
    """
    rows = [
        (athlete_id, a["id"], extracted_at_utc, json.dumps(a))
        for a in activities
    ]

    sql = """
        insert into raw.strava_activities (athlete_id, activity_id, extracted_at_utc, payload)
        values %s
        on conflict (athlete_id, activity_id) do update
          set extracted_at_utc = excluded.extracted_at_utc,
              payload = excluded.payload;
    """

    execute_values(cur, sql, rows, page_size=1000)
    return len(rows)


# -----------------------------
# Token validity logic
# -----------------------------

def ensure_valid_access_token(
    cur,
    client_id: str,
    client_secret: str,
    row: TokenRow,
) -> TokenRow:
    """
    If token missing/expired => refresh + store in DB.
    Otherwise return as-is.
    """
    now = int(time.time())

    if (not row.access_token) or (now >= row.expires_at):
        print(f"[{row.athlete_id}] token expired/missing -> refreshing")
        token_data = refresh_access_token(client_id, client_secret, row.refresh_token)

        new_row = TokenRow(
            athlete_id=row.athlete_id,
            access_token=token_data["access_token"],
            refresh_token=token_data["refresh_token"],
            expires_at=int(token_data["expires_at"]),
        )

        upsert_token(cur, new_row.athlete_id, new_row.access_token, new_row.refresh_token, new_row.expires_at)
        return new_row

    return row


# -----------------------------
# Main
# -----------------------------

def main() -> None:
    # For GitHub Actions / Render, env vars are typically injected, so .env is optional.
    # If you still use .env locally, you can add: from dotenv import load_dotenv; load_dotenv()

    db_url = require_env("DATABASE_URL")
    client_id = require_env("STRAVA_CLIENT_ID")
    client_secret = require_env("STRAVA_CLIENT_SECRET")

    extracted_at = utc_now_iso()

    conn = psycopg2.connect(db_url)
    try:
        with conn:
            with conn.cursor() as cur:
                ensure_raw_table(cur)

                token_rows = fetch_all_tokens(cur)
                if not token_rows:
                    print("No athletes found in strava_token.")
                    return

                for row in token_rows:
                    try:
                        valid_row = ensure_valid_access_token(cur, client_id, client_secret, row)
                        activities = fetch_activities(valid_row.access_token, per_page=200)
                        count = upsert_activities(cur, valid_row.athlete_id, extracted_at, activities)
                        print(f"[{valid_row.athlete_id}] loaded {count} activities")
                    except Exception as e:
                        # Continue other athletes if one fails.
                        print(f"[{row.athlete_id}] ERROR: {e}")

    finally:
        conn.close()


if __name__ == "__main__":
    main()
