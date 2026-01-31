# AI refactored code, need to validate
import json
import os
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Dict, List, Tuple

import requests
import psycopg2
from psycopg2.extras import execute_values


# -----------------------------
# Tables (schema-qualified)
# -----------------------------
TOKEN_TABLE = "public.strava_tokens"
RAW_TABLE = "raw.strava_activities"


# -----------------------------
# Helpers
# -----------------------------
def utc_now_iso() -> str:
    """UTC timestamp for extracted_at_utc (same for whole run)."""
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def require_env(name: str) -> str:
    """Read env var or fail loudly."""
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
    Refresh Strava access token using refresh token.

    Important: Strava can rotate refresh tokens; always store returned refresh_token.
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

    # Include response body for easier debugging in CI logs
    if resp.status_code != 200:
        raise RuntimeError(f"Token refresh failed {resp.status_code}: {resp.text}")

    return resp.json()


def fetch_activities(access_token: str, per_page: int = 200) -> List[Dict[str, Any]]:
    """
    Fetch ALL activities for the authenticated athlete using paging.

    Notes:
    - If you later want incremental loading, add Strava's 'after' parameter
      and store a watermark per athlete in your DB.
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

        # Basic rate-limit handling
        if resp.status_code == 429:
            time.sleep(5)
            continue

        if resp.status_code == 401:
            raise RuntimeError("Strava API 401 Unauthorized (token revoked/invalid)")

        if resp.status_code != 200:
            raise RuntimeError(f"Strava API error {resp.status_code}: {resp.text}")

        data = resp.json()
        if not data:
            break

        all_activities.extend(data)
        page += 1

    return all_activities


# -----------------------------
# Postgres (Neon) functions
# -----------------------------
def ensure_raw_table(cur) -> None:
    """
    Ensure raw schema + table exists.

    We store:
    - athlete_id: who the activity belongs to
    - activity_id: Strava activity ID
    - extracted_at_utc: when we ingested it (run timestamp)
    - payload: full Strava JSON for the activity (jsonb)
    """
    cur.execute("create schema if not exists raw;")
    cur.execute(
        f"""
        create table if not exists {RAW_TABLE} (
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
    Read all athletes from the tokens table.
    If this returns 0 rows, ingestion has nothing to do.
    """
    cur.execute(
        f"""
        select athlete_id, access_token, refresh_token, expires_at
        from {TOKEN_TABLE}
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
    Persist refreshed token values back into TOKEN_TABLE.

    Assumes athlete_id is unique / primary key in public.strava_tokens.
    """
    cur.execute(
        f"""
        insert into {TOKEN_TABLE} (athlete_id, access_token, refresh_token, expires_at, updated_at)
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
    Bulk upsert activities into RAW_TABLE.

    execute_values builds one INSERT with many rows (fast),
    while still using ON CONFLICT for idempotency.
    """
    if not activities:
        return 0

    # Guard against unexpected payloads missing "id"
    rows = [
        (athlete_id, a["id"], extracted_at_utc, json.dumps(a, ensure_ascii=False))
        for a in activities
        if isinstance(a, dict) and "id" in a
    ]

    if not rows:
        return 0

    sql = f"""
        insert into {RAW_TABLE} (athlete_id, activity_id, extracted_at_utc, payload)
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
def ensure_valid_access_token(cur, client_id: str, client_secret: str, row: TokenRow) -> TokenRow:
    """
    If access token is missing or expired, refresh it and store new values in DB.
    Otherwise return the existing row unchanged.
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
    """
    Entry point for CI/CD / cron.

    Required env vars:
    - DATABASE_URL
    - STRAVA_CLIENT_ID
    - STRAVA_CLIENT_SECRET
    """
    db_url = require_env("DATABASE_URL")
    client_id = require_env("STRAVA_CLIENT_ID")
    client_secret = require_env("STRAVA_CLIENT_SECRET")

    extracted_at = utc_now_iso()
    print("INGESTION START UTC:", extracted_at)
    print("TOKEN_TABLE:", TOKEN_TABLE)
    print("RAW_TABLE:", RAW_TABLE)

    conn = psycopg2.connect(db_url)

    total_upserted = 0
    errors: List[Tuple[int, str]] = []

    try:
        with conn:
            with conn.cursor() as cur:
                # Print DB identity so you can confirm you're looking at the same DB in Neon UI.
                cur.execute("select current_database(), current_user, inet_server_addr(), now() at time zone 'utc';")
                print("DB IDENTITY:", cur.fetchone())

                ensure_raw_table(cur)

                token_rows = fetch_all_tokens(cur)
                print(f"Found {len(token_rows)} token row(s).")

                if not token_rows:
                    raise RuntimeError(
                        f"No athletes found in {TOKEN_TABLE}. "
                        "Either tokens were not stored, or you are connected to the wrong database."
                    )

                ok_athletes = 0

                for row in token_rows:
                    try:
                        valid_row = ensure_valid_access_token(cur, client_id, client_secret, row)
                        activities = fetch_activities(valid_row.access_token, per_page=200)
                        upserted = upsert_activities(cur, valid_row.athlete_id, extracted_at, activities)

                        total_upserted += upserted
                        ok_athletes += 1

                        print(f"[{valid_row.athlete_id}] fetched={len(activities)} upserted={upserted}")

                    except Exception as e:
                        # Keep going so one bad athlete doesn't block the rest,
                        # but record it and potentially fail if nothing was inserted overall.
                        errors.append((row.athlete_id, str(e)))
                        print(f"[{row.athlete_id}] ERROR: {e}")

                print(f"SUMMARY: athletes_ok={ok_athletes}/{len(token_rows)} total_upserted={total_upserted}")

                # Critical: fail workflow if nothing landed
                if total_upserted == 0:
                    raise RuntimeError(f"Ingestion inserted 0 rows. Sample errors: {errors[:3]}")

    finally:
        conn.close()


if __name__ == "__main__":
    main()
