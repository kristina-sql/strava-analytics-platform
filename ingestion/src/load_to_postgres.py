import json
import os
from pathlib import Path

import psycopg2
from dotenv import load_dotenv


def latest_raw_json(raw_dir: Path) -> Path:
    files = sorted(raw_dir.glob("strava_activities_raw_*.json"))
    if not files:
        raise FileNotFoundError(f"No raw JSON snapshots found in: {raw_dir}")
    return files[-1]


def main() -> None:
    load_dotenv()

    db_url = os.getenv("DATABASE_URL")
    if not db_url:
        raise RuntimeError("Missing DATABASE_URL in .env (Neon connection string)")

    snapshot_path = latest_raw_json(Path("data") / "raw")

    doc = json.loads(snapshot_path.read_text())
    extracted_at = doc["extracted_at_utc"]
    activities = doc["activities"]

    conn = psycopg2.connect(db_url)
    conn.autocommit = False

    with conn.cursor() as cur:
        cur.execute("create schema if not exists raw;")
        cur.execute(
            """
            create table if not exists raw.strava_activities (
              activity_id bigint primary key,
              extracted_at_utc timestamptz not null,
              payload jsonb not null
            );
            """
        )

        upsert_sql = """
            insert into raw.strava_activities (activity_id, extracted_at_utc, payload)
            values (%s, %s, %s::jsonb)
            on conflict (activity_id) do update
              set extracted_at_utc = excluded.extracted_at_utc,
                  payload = excluded.payload;
        """

        for a in activities:
            cur.execute(upsert_sql, (a["id"], extracted_at, json.dumps(a)))

    conn.commit()
    conn.close()

    print(f"Loaded {len(activities)} activities into Neon raw.strava_activities")
    print(f"Source snapshot: {snapshot_path}")


if __name__ == "__main__":
    main()
