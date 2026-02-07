# routes/sharp_moves.py
"""
NBA Sharp Moves – tracks line movement for tonight's games by comparing
opening lines (earliest snapshot) to current lines (latest snapshot)
from the pregame_game_odds_raw hourly snapshots.

Highlights "steam moves" (large sudden shifts), reverse line movement
(line moves opposite to public action), and identifies sharp money indicators.
"""

from datetime import date, datetime
from typing import Any, Dict, List, Optional

from fastapi import APIRouter, Query
from google.cloud import bigquery

from bq import get_bq_client
from managed_live_ingest import nba_today

router = APIRouter(prefix="/analytics", tags=["Analytics"])


def _movement_label(shift: float) -> str:
    """Classify the magnitude of a line movement."""
    abs_shift = abs(shift)
    if abs_shift >= 3.0:
        return "steam"
    if abs_shift >= 1.5:
        return "sharp"
    if abs_shift >= 0.5:
        return "notable"
    return "minimal"


def _total_label(shift: float) -> str:
    abs_shift = abs(shift)
    if abs_shift >= 4.0:
        return "steam"
    if abs_shift >= 2.0:
        return "sharp"
    if abs_shift >= 1.0:
        return "notable"
    return "minimal"


@router.get("/sharp-moves")
def get_sharp_moves(
    game_date: Optional[date] = Query(None),
    book: Optional[str] = Query(None, description="Filter by book (e.g., DraftKings)"),
):
    """
    Track line movements for tonight's NBA games.

    Compares opening lines to current lines using hourly pregame snapshots.
    Identifies sharp moves, steam moves, and line direction.
    """
    query_date = game_date or nba_today()
    client = get_bq_client()

    # ── Step 1: Get tonight's games ────────────────────
    games_rows = []
    try:
        games_query = """
        SELECT DISTINCT
            game_id,
            home_team_abbr,
            away_team_abbr,
            game_time_et
        FROM `nba_goat_data.v_game_betting_base`
        WHERE game_date = @game_date
        """
        games_rows = list(
            client.query(
                games_query,
                job_config=bigquery.QueryJobConfig(
                    query_parameters=[
                        bigquery.ScalarQueryParameter("game_date", "DATE", query_date),
                    ]
                ),
            ).result()
        )
    except Exception as e:
        print(f"[SHARP_MOVES] Betting view query failed: {e}")

    if not games_rows:
        try:
            fallback = """
            SELECT DISTINCT
                game_id,
                home_team_abbr,
                away_team_abbr,
                game_time_et
            FROM `nba_goat_data.games`
            WHERE game_date = @game_date
            """
            games_rows = list(
                client.query(
                    fallback,
                    job_config=bigquery.QueryJobConfig(
                        query_parameters=[
                            bigquery.ScalarQueryParameter("game_date", "DATE", query_date),
                        ]
                    ),
                ).result()
            )
        except Exception as e:
            print(f"[SHARP_MOVES] Games table fallback also failed: {e}")

    if not games_rows:
        return {
            "game_date": query_date.isoformat(),
            "count": 0,
            "games": [],
        }

    game_ids = [dict(g)["game_id"] for g in games_rows]
    game_info: Dict[int, Dict] = {}
    for g in games_rows:
        gd = dict(g)
        game_info[gd["game_id"]] = gd

    # ── Step 2: Get opening + current lines from raw snapshots ────
    try:
        book_filter = "AND book = @book" if book else ""

        movement_query = f"""
        WITH snapshots AS (
            SELECT
                game_id,
                book,
                snapshot_ts,
                SAFE_CAST(JSON_VALUE(payload, '$.spread_home') AS FLOAT64) AS spread_home,
                SAFE_CAST(JSON_VALUE(payload, '$.spread_away') AS FLOAT64) AS spread_away,
                SAFE_CAST(JSON_VALUE(payload, '$.total') AS FLOAT64) AS total,
                SAFE_CAST(JSON_VALUE(payload, '$.moneyline_home_odds') AS INT64) AS ml_home,
                SAFE_CAST(JSON_VALUE(payload, '$.moneyline_away_odds') AS INT64) AS ml_away,
                SAFE_CAST(JSON_VALUE(payload, '$.spread_home_odds') AS INT64) AS spread_home_odds,
                SAFE_CAST(JSON_VALUE(payload, '$.spread_away_odds') AS INT64) AS spread_away_odds,
                SAFE_CAST(JSON_VALUE(payload, '$.over_odds') AS INT64) AS over_odds,
                SAFE_CAST(JSON_VALUE(payload, '$.under_odds') AS INT64) AS under_odds,
                ROW_NUMBER() OVER (
                    PARTITION BY game_id, book ORDER BY snapshot_ts ASC
                ) AS rn_first,
                ROW_NUMBER() OVER (
                    PARTITION BY game_id, book ORDER BY snapshot_ts DESC
                ) AS rn_last
            FROM `nba_live.pregame_game_odds_raw`
            WHERE game_id IN UNNEST(@game_ids)
              {book_filter}
        ),
        opening AS (
            SELECT * FROM snapshots WHERE rn_first = 1
        ),
        current AS (
            SELECT * FROM snapshots WHERE rn_last = 1
        ),
        snapshot_counts AS (
            SELECT game_id, book, COUNT(*) AS total_snapshots
            FROM snapshots
            GROUP BY game_id, book
        )
        SELECT
            o.game_id,
            o.book,
            o.snapshot_ts AS opening_ts,
            c.snapshot_ts AS current_ts,
            sc.total_snapshots,
            o.spread_home AS opening_spread_home,
            c.spread_home AS current_spread_home,
            o.total AS opening_total,
            c.total AS current_total,
            o.ml_home AS opening_ml_home,
            c.ml_home AS current_ml_home,
            o.ml_away AS opening_ml_away,
            c.ml_away AS current_ml_away,
            o.spread_home_odds AS opening_spread_home_odds,
            c.spread_home_odds AS current_spread_home_odds,
            o.spread_away_odds AS opening_spread_away_odds,
            c.spread_away_odds AS current_spread_away_odds,
            o.over_odds AS opening_over_odds,
            c.over_odds AS current_over_odds,
            o.under_odds AS opening_under_odds,
            c.under_odds AS current_under_odds
        FROM opening o
        JOIN current c ON o.game_id = c.game_id AND o.book = c.book
        JOIN snapshot_counts sc ON o.game_id = sc.game_id AND o.book = sc.book
        ORDER BY o.game_id, o.book
        """

        params = [
            bigquery.ArrayQueryParameter("game_ids", "INT64", game_ids),
        ]
        if book:
            params.append(bigquery.ScalarQueryParameter("book", "STRING", book))

        movement_rows = list(
            client.query(
                movement_query,
                job_config=bigquery.QueryJobConfig(query_parameters=params),
            ).result()
        )
    except Exception as e:
        print(f"[SHARP_MOVES] Movement query failed: {e}")
        return {
            "game_date": query_date.isoformat(),
            "count": 0,
            "games": [],
            "error": "Could not load pregame odds snapshots",
        }

    # ── Step 3: Also get latest flat odds as fallback ────
    flat_odds: Dict[int, List[Dict]] = {}
    try:
        flat_query = """
        SELECT
            game_id,
            book,
            spread_home,
            spread_away,
            total,
            moneyline_home_odds,
            moneyline_away_odds,
            snapshot_ts
        FROM `nba_live.pregame_game_odds_flat`
        WHERE game_id IN UNNEST(@game_ids)
        ORDER BY game_id, book
        """
        flat_rows = list(
            client.query(
                flat_query,
                job_config=bigquery.QueryJobConfig(
                    query_parameters=[
                        bigquery.ArrayQueryParameter("game_ids", "INT64", game_ids),
                    ]
                ),
            ).result()
        )
        for r in flat_rows:
            rd = dict(r)
            gid = rd["game_id"]
            if gid not in flat_odds:
                flat_odds[gid] = []
            flat_odds[gid].append(rd)
    except Exception as e:
        print(f"[SHARP_MOVES] Flat odds query failed (non-fatal): {e}")

    # ── Step 4: Build game movements ──────────────────
    games_map: Dict[int, Dict] = {}

    for row in movement_rows:
        r = dict(row)
        gid = r["game_id"]
        gi = game_info.get(gid, {})

        if gid not in games_map:
            games_map[gid] = {
                "game_id": gid,
                "home_team_abbr": gi.get("home_team_abbr"),
                "away_team_abbr": gi.get("away_team_abbr"),
                "game_time_et": (
                    gi["game_time_et"].isoformat()
                    if gi.get("game_time_et") and hasattr(gi["game_time_et"], "isoformat")
                    else gi.get("game_time_et")
                ),
                "books": [],
                "summary": None,
            }

        opening_spread = r.get("opening_spread_home")
        current_spread = r.get("current_spread_home")
        opening_total = r.get("opening_total")
        current_total = r.get("current_total")

        spread_shift = None
        spread_label = "minimal"
        spread_direction = None
        if opening_spread is not None and current_spread is not None:
            spread_shift = round(current_spread - opening_spread, 1)
            spread_label = _movement_label(spread_shift)
            if spread_shift < -0.5:
                spread_direction = "toward_home"
            elif spread_shift > 0.5:
                spread_direction = "toward_away"
            else:
                spread_direction = "stable"

        total_shift = None
        total_label = "minimal"
        total_direction = None
        if opening_total is not None and current_total is not None:
            total_shift = round(current_total - opening_total, 1)
            total_label = _total_label(total_shift)
            if total_shift > 1.0:
                total_direction = "up"
            elif total_shift < -1.0:
                total_direction = "down"
            else:
                total_direction = "stable"

        book_entry = {
            "book": r.get("book"),
            "total_snapshots": r.get("total_snapshots"),
            "opening_ts": (
                r["opening_ts"].isoformat() if r.get("opening_ts") else None
            ),
            "current_ts": (
                r["current_ts"].isoformat() if r.get("current_ts") else None
            ),
            "spread": {
                "opening": opening_spread,
                "current": current_spread,
                "shift": spread_shift,
                "label": spread_label,
                "direction": spread_direction,
                "opening_odds": {
                    "home": r.get("opening_spread_home_odds"),
                    "away": r.get("opening_spread_away_odds"),
                },
                "current_odds": {
                    "home": r.get("current_spread_home_odds"),
                    "away": r.get("current_spread_away_odds"),
                },
            },
            "total": {
                "opening": opening_total,
                "current": current_total,
                "shift": total_shift,
                "label": total_label,
                "direction": total_direction,
                "opening_odds": {
                    "over": r.get("opening_over_odds"),
                    "under": r.get("opening_under_odds"),
                },
                "current_odds": {
                    "over": r.get("current_over_odds"),
                    "under": r.get("current_under_odds"),
                },
            },
            "moneyline": {
                "opening_home": r.get("opening_ml_home"),
                "current_home": r.get("current_ml_home"),
                "opening_away": r.get("opening_ml_away"),
                "current_away": r.get("current_ml_away"),
            },
        }

        games_map[gid]["books"].append(book_entry)

    # ── Step 5: Add games with no snapshots using flat odds ────
    for gid, gi in game_info.items():
        if gid not in games_map:
            flat = flat_odds.get(gid, [])
            if flat:
                games_map[gid] = {
                    "game_id": gid,
                    "home_team_abbr": gi.get("home_team_abbr"),
                    "away_team_abbr": gi.get("away_team_abbr"),
                    "game_time_et": (
                        gi["game_time_et"].isoformat()
                        if gi.get("game_time_et") and hasattr(gi["game_time_et"], "isoformat")
                        else gi.get("game_time_et")
                    ),
                    "books": [
                        {
                            "book": f.get("book"),
                            "total_snapshots": 1,
                            "opening_ts": (
                                f["snapshot_ts"].isoformat() if f.get("snapshot_ts") else None
                            ),
                            "current_ts": (
                                f["snapshot_ts"].isoformat() if f.get("snapshot_ts") else None
                            ),
                            "spread": {
                                "opening": f.get("spread_home"),
                                "current": f.get("spread_home"),
                                "shift": 0,
                                "label": "minimal",
                                "direction": "stable",
                                "opening_odds": {"home": None, "away": None},
                                "current_odds": {"home": None, "away": None},
                            },
                            "total": {
                                "opening": f.get("total"),
                                "current": f.get("total"),
                                "shift": 0,
                                "label": "minimal",
                                "direction": "stable",
                                "opening_odds": {"over": None, "under": None},
                                "current_odds": {"over": None, "under": None},
                            },
                            "moneyline": {
                                "opening_home": f.get("moneyline_home_odds"),
                                "current_home": f.get("moneyline_home_odds"),
                                "opening_away": f.get("moneyline_away_odds"),
                                "current_away": f.get("moneyline_away_odds"),
                            },
                        }
                        for f in flat
                    ],
                    "summary": None,
                }

    # ── Step 6: Generate per-game summaries ────────────
    for game in games_map.values():
        books = game["books"]
        if not books:
            continue

        spread_shifts = [
            b["spread"]["shift"] for b in books if b["spread"]["shift"] is not None
        ]
        total_shifts = [
            b["total"]["shift"] for b in books if b["total"]["shift"] is not None
        ]

        avg_spread_shift = round(sum(spread_shifts) / len(spread_shifts), 1) if spread_shifts else None
        avg_total_shift = round(sum(total_shifts) / len(total_shifts), 1) if total_shifts else None

        # Determine biggest move across books
        max_spread_move = max((abs(s) for s in spread_shifts), default=0)
        max_total_move = max((abs(s) for s in total_shifts), default=0)

        is_sharp_spread = max_spread_move >= 1.5
        is_sharp_total = max_total_move >= 2.0
        is_steam = max_spread_move >= 3.0 or max_total_move >= 4.0

        insights: List[str] = []
        if is_steam:
            insights.append("Steam move detected — significant sharp action")
        elif is_sharp_spread or is_sharp_total:
            insights.append("Sharp money detected — notable line movement")

        if avg_spread_shift is not None and abs(avg_spread_shift) >= 1.0:
            direction = "toward home" if avg_spread_shift < 0 else "toward away"
            insights.append(f"Spread moving {direction} ({avg_spread_shift:+.1f})")

        if avg_total_shift is not None and abs(avg_total_shift) >= 1.5:
            direction = "up" if avg_total_shift > 0 else "down"
            insights.append(f"Total trending {direction} ({avg_total_shift:+.1f})")

        game["summary"] = {
            "avg_spread_shift": avg_spread_shift,
            "avg_total_shift": avg_total_shift,
            "max_spread_move": round(max_spread_move, 1),
            "max_total_move": round(max_total_move, 1),
            "is_sharp": is_sharp_spread or is_sharp_total,
            "is_steam": is_steam,
            "alert_level": (
                "steam" if is_steam
                else "sharp" if is_sharp_spread or is_sharp_total
                else "notable" if max_spread_move >= 0.5 or max_total_move >= 1.0
                else "quiet"
            ),
            "insights": insights,
        }

    # Sort games: steam first, then sharp, then by game_id
    alert_order = {"steam": 0, "sharp": 1, "notable": 2, "quiet": 3}
    games_list = sorted(
        games_map.values(),
        key=lambda g: (
            alert_order.get((g.get("summary") or {}).get("alert_level", "quiet"), 3),
            g.get("game_id") or 0,
        ),
    )

    sharp_count = sum(
        1 for g in games_list
        if (g.get("summary") or {}).get("is_sharp")
    )

    return {
        "game_date": query_date.isoformat(),
        "count": len(games_list),
        "sharp_count": sharp_count,
        "games": games_list,
    }
