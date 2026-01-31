# pregame_game_odds_ingest.py
"""
Pre-game game odds ingestion - fetches odds for UPCOMING games.

This runs hourly until game time, capturing pre-game spread, moneyline,
and totals from DraftKings and FanDuel.

Tables:
- pregame_game_odds_raw: All hourly snapshots (for historical analysis)
- pregame_game_odds_flat: Latest pre-game odds per game/book
- closing_lines: Final odds captured when game transitions to LIVE

Differs from live_game_odds_ingest.py which only runs during live games.
"""

from datetime import datetime, timezone
import json
import requests
import os

from google.cloud import bigquery

from LiveOdds.live_odds_common import (
    BDL_V2,
    TIMEOUT_SEC,
    require_api_key,
    get_bq_client,
    LIVE_ODDS_BOOKS,
    normalize_book,
)

# BigQuery tables
BQ_TABLE_RAW = "graphite-flare-477419-h7.nba_live.pregame_game_odds_raw"
BQ_TABLE_FLAT = "graphite-flare-477419-h7.nba_live.pregame_game_odds_flat"
BQ_TABLE_CLOSING = "graphite-flare-477419-h7.nba_live.closing_lines"

PROJECT_ID = os.getenv("GCP_PROJECT", "graphite-flare-477419-h7")


def fetch_upcoming_game_ids() -> list[int]:
    """
    Fetch game IDs for upcoming games from BigQuery.

    Returns:
        List of game IDs that are in UPCOMING state
    """
    client = get_bq_client()

    rows = list(
        client.query(
            """
            SELECT DISTINCT game_id
            FROM `graphite-flare-477419-h7.nba_live.live_games`
            WHERE state = 'UPCOMING'
            """
        ).result()
    )

    game_ids = [r.game_id for r in rows]
    print(f"[PREGAME_ODDS] Found {len(game_ids)} upcoming games: {game_ids}")

    return game_ids


def ingest_pregame_game_odds() -> dict:
    """
    Pull pre-game betting odds for UPCOMING games
    from DraftKings and FanDuel.

    Returns:
        Dict with status and count of games ingested
    """

    upcoming_game_ids = fetch_upcoming_game_ids()
    if not upcoming_game_ids:
        return {"status": "SKIPPED", "reason": "no upcoming games"}

    headers = {
        "Authorization": f"Bearer {require_api_key()}",
        "Accept": "application/json",
    }

    resp = requests.get(
        f"{BDL_V2}/odds",
        headers=headers,
        params={"game_ids[]": upcoming_game_ids},
        timeout=TIMEOUT_SEC,
    )
    resp.raise_for_status()

    payload = resp.json()
    now = datetime.now(timezone.utc)

    rows = []
    games_processed = set()

    for game in payload.get("data", []):
        game_id = game.get("game_id")
        if not game_id:
            continue

        book = normalize_book(game.get("vendor"))
        if book not in LIVE_ODDS_BOOKS:
            continue

        games_processed.add(game_id)

        rows.append(
            {
                "snapshot_ts": now.isoformat(),
                "game_id": game_id,
                "book": book,
                "payload": json.dumps({
                    "book": book,

                    "spread_home": game.get("spread_home_value"),
                    "spread_home_odds": game.get("spread_home_odds"),
                    "spread_away": game.get("spread_away_value"),
                    "spread_away_odds": game.get("spread_away_odds"),

                    "total": game.get("total_value"),
                    "total_over_odds": game.get("total_over_odds"),
                    "total_under_odds": game.get("total_under_odds"),

                    "moneyline_home_odds": game.get("moneyline_home_odds"),
                    "moneyline_away_odds": game.get("moneyline_away_odds"),

                    "updated_at": game.get("updated_at"),
                }),
            }
        )

    if rows:
        client = get_bq_client()
        errors = client.insert_rows_json(BQ_TABLE_RAW, rows)
        if errors:
            raise RuntimeError(f"Pre-game odds insert errors: {errors}")

    result = {
        "status": "OK",
        "games_with_odds": len(games_processed),
        "rows_inserted": len(rows),
        "snapshot_ts": now.isoformat(),
    }

    print(f"[PREGAME_ODDS] Ingested {len(rows)} rows for {len(games_processed)} games")

    return result


# ======================================================
# FLATTEN SQL - Updates latest pre-game odds per game/book
# ======================================================

PREGAME_FLATTEN_SQL = """
MERGE `graphite-flare-477419-h7.nba_live.pregame_game_odds_flat` T
USING (
  SELECT *
  FROM (
    SELECT
      TIMESTAMP(snapshot_ts) AS snapshot_ts,
      game_id,
      book,

      CAST(JSON_VALUE(payload, '$.spread_home') AS FLOAT64) AS spread_home,
      CAST(JSON_VALUE(payload, '$.spread_away') AS FLOAT64) AS spread_away,

      CAST(JSON_VALUE(payload, '$.spread_home_odds') AS INT64) AS spread_home_odds,
      CAST(JSON_VALUE(payload, '$.spread_away_odds') AS INT64) AS spread_away_odds,

      CAST(JSON_VALUE(payload, '$.total') AS FLOAT64) AS total,
      CAST(JSON_VALUE(payload, '$.total_over_odds') AS INT64) AS over_odds,
      CAST(JSON_VALUE(payload, '$.total_under_odds') AS INT64) AS under_odds,

      CAST(JSON_VALUE(payload, '$.moneyline_home_odds') AS INT64) AS moneyline_home_odds,
      CAST(JSON_VALUE(payload, '$.moneyline_away_odds') AS INT64) AS moneyline_away_odds,

      ROW_NUMBER() OVER (
        PARTITION BY game_id, book
        ORDER BY TIMESTAMP(snapshot_ts) DESC
      ) AS rn
    FROM `graphite-flare-477419-h7.nba_live.pregame_game_odds_raw`
  )
  WHERE rn = 1
) S
ON
  T.game_id = S.game_id
  AND T.book = S.book

WHEN MATCHED THEN
  UPDATE SET
    snapshot_ts            = S.snapshot_ts,
    spread_home            = S.spread_home,
    spread_away            = S.spread_away,
    spread_home_odds       = S.spread_home_odds,
    spread_away_odds       = S.spread_away_odds,
    total                  = S.total,
    over_odds              = S.over_odds,
    under_odds             = S.under_odds,
    moneyline_home_odds    = S.moneyline_home_odds,
    moneyline_away_odds    = S.moneyline_away_odds

WHEN NOT MATCHED THEN
  INSERT (
    snapshot_ts,
    game_id,
    book,
    spread_home,
    spread_away,
    spread_home_odds,
    spread_away_odds,
    total,
    over_odds,
    under_odds,
    moneyline_home_odds,
    moneyline_away_odds
  )
  VALUES (
    S.snapshot_ts,
    S.game_id,
    S.book,
    S.spread_home,
    S.spread_away,
    S.spread_home_odds,
    S.spread_away_odds,
    S.total,
    S.over_odds,
    S.under_odds,
    S.moneyline_home_odds,
    S.moneyline_away_odds
  );
"""


# ======================================================
# CLOSING LINE CAPTURE SQL
# Captures the last pre-game odds for games that just went LIVE
# Only inserts games that don't already have closing lines
# ======================================================

CLOSING_LINE_CAPTURE_SQL = """
INSERT INTO `graphite-flare-477419-h7.nba_live.closing_lines` (
  game_id,
  game_date,
  book,
  captured_at,
  spread_home,
  spread_away,
  spread_home_odds,
  spread_away_odds,
  total,
  over_odds,
  under_odds,
  moneyline_home_odds,
  moneyline_away_odds
)
SELECT
  p.game_id,
  g.game_date,
  p.book,
  CURRENT_TIMESTAMP() AS captured_at,
  p.spread_home,
  p.spread_away,
  p.spread_home_odds,
  p.spread_away_odds,
  p.total,
  p.over_odds,
  p.under_odds,
  p.moneyline_home_odds,
  p.moneyline_away_odds
FROM `graphite-flare-477419-h7.nba_live.pregame_game_odds_flat` p
JOIN `graphite-flare-477419-h7.nba_live.live_games` g
  ON p.game_id = g.game_id
WHERE g.state = 'LIVE'
  AND NOT EXISTS (
    SELECT 1
    FROM `graphite-flare-477419-h7.nba_live.closing_lines` c
    WHERE c.game_id = p.game_id
      AND c.book = p.book
  );
"""


def run_pregame_odds_flatten():
    """
    Idempotently flatten pre-game odds RAW table
    into the FLAT table (latest per game/book).

    Safe to run hourly or more frequently.
    """
    client = bigquery.Client(project=PROJECT_ID)

    print("[PREGAME_ODDS] Flattening pre-game odds (idempotent)")
    client.query(PREGAME_FLATTEN_SQL).result()

    print("[PREGAME_ODDS] Pre-game odds flatten complete")


def capture_closing_lines() -> dict:
    """
    Capture closing lines for games that just transitioned to LIVE.

    This should be called when games start to preserve the final
    pre-game odds for ATS tracking.

    Returns:
        Dict with number of closing lines captured
    """
    client = bigquery.Client(project=PROJECT_ID)

    print("[PREGAME_ODDS] Capturing closing lines for newly live games")

    # Run the capture SQL
    job = client.query(CLOSING_LINE_CAPTURE_SQL)
    result = job.result()

    # Get number of rows inserted
    rows_inserted = job.num_dml_affected_rows or 0

    print(f"[PREGAME_ODDS] Captured {rows_inserted} closing line rows")

    return {
        "status": "OK",
        "closing_lines_captured": rows_inserted,
    }


def run_full_pregame_cycle() -> dict:
    """
    Run full pre-game odds cycle:
    1. Ingest latest odds for upcoming games
    2. Flatten to get latest per game/book
    3. Capture closing lines for games that just went live

    Returns:
        Dict with results from each step
    """
    results = {
        "ingest": None,
        "flatten": None,
        "closing_lines": None,
    }

    # 1. Ingest
    try:
        results["ingest"] = ingest_pregame_game_odds()
    except Exception as e:
        print(f"[PREGAME_ODDS] ERROR in ingest: {e}")
        results["ingest"] = {"status": "ERROR", "error": str(e)}

    # 2. Flatten
    try:
        run_pregame_odds_flatten()
        results["flatten"] = {"status": "OK"}
    except Exception as e:
        print(f"[PREGAME_ODDS] ERROR in flatten: {e}")
        results["flatten"] = {"status": "ERROR", "error": str(e)}

    # 3. Capture closing lines
    try:
        results["closing_lines"] = capture_closing_lines()
    except Exception as e:
        print(f"[PREGAME_ODDS] ERROR capturing closing lines: {e}")
        results["closing_lines"] = {"status": "ERROR", "error": str(e)}

    return results


if __name__ == "__main__":
    result = run_full_pregame_cycle()
    print(f"Result: {result}")
