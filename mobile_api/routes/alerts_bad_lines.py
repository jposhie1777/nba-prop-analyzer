# alerts_bad_lines.py
from fastapi import APIRouter
from google.cloud import bigquery

from bq import get_bq_client
from routes.push import send_push

router = APIRouter(prefix="/alerts", tags=["alerts"])


@router.post("/bad-lines/check")
def check_bad_line_alerts(
    min_score: float = 1.25,
    max_lines: int = 5,
):
    bq = get_bq_client()

    # -------------------------------------------------
    # 1. Fetch NEW bad lines (deduped + filtered)
    # -------------------------------------------------
    bad_lines = list(
        bq.query(
            """
            WITH ranked AS (
              SELECT
                bl.*,
                ROW_NUMBER() OVER (
                  PARTITION BY bl.player_id, bl.market, bl.line_value
                  ORDER BY bl.bad_line_score DESC
                ) AS rn
              FROM nba_live.v_bad_line_alerts_scored bl
              WHERE bl.is_bad_line = TRUE
                AND bl.bad_line_score >= @min_score
                AND bl.odds BETWEEN -150 AND 300
                AND LOWER(bl.book) IN ('draftkings', 'fanduel')
                AND NOT EXISTS (
                  SELECT 1
                  FROM nba_live.bad_line_alert_log l
                  WHERE l.player_id = bl.player_id
                    AND l.market = bl.market
                    AND l.line_value = bl.line_value
                )
            )

            SELECT *
            FROM ranked
            WHERE rn = 1
            ORDER BY bad_line_score DESC
            LIMIT @max_lines
            """,
            job_config=bigquery.QueryJobConfig(
                query_parameters=[
                    bigquery.ScalarQueryParameter(
                        "min_score", "FLOAT", min_score
                    ),
                    bigquery.ScalarQueryParameter(
                        "max_lines", "INT64", max_lines
                    ),
                ]
            ),
        )
    )

    if not bad_lines:
        return {"ok": True, "sent": 0, "reason": "no new bad lines"}

    # -------------------------------------------------
    # 2. Fetch push tokens
    # -------------------------------------------------
    tokens = list(
        bq.query("SELECT expo_push_token FROM nba_live.push_tokens")
    )

    sent = 0
    failures = 0

    # -------------------------------------------------
    # 3. Send push notifications
    # -------------------------------------------------
    for row in bad_lines:
        for t in tokens:
            try:
                send_push(
                    token=t["expo_push_token"],
                    title="Bad Line Detected",
                    body=(
                        f"{row['player_name']} "
                        f"{row['market']} {row['line_value']} "
                        f"({row['odds']:+})"
                    ),
                    data={
                        "type": "bad_line",
                        "player_id": row["player_id"],
                        "market": row["market"],
                        "line_value": row["line_value"],
                        "book": row["book"],
                        "odds": row["odds"],
                        "score": row["bad_line_score"],
                    },
                )

                sent += 1

                # ----------------------------------
                # 4. Log alert (true dedupe key)
                # ----------------------------------
                bq.query(
                    """
                    INSERT INTO nba_live.bad_line_alert_log (
                        player_id,
                        market,
                        line_value,
                        prop_id,
                        book,
                        expo_push_token
                    )
                    VALUES (
                        @player_id,
                        @market,
                        @line_value,
                        @prop_id,
                        @book,
                        @token
                    )
                    """,
                    job_config=bigquery.QueryJobConfig(
                        query_parameters=[
                            bigquery.ScalarQueryParameter(
                                "player_id", "INT64", row["player_id"]
                            ),
                            bigquery.ScalarQueryParameter(
                                "market", "STRING", row["market"]
                            ),
                            bigquery.ScalarQueryParameter(
                                "line_value", "FLOAT", row["line_value"]
                            ),
                            bigquery.ScalarQueryParameter(
                                "prop_id", "STRING", row["prop_id"]
                            ),
                            bigquery.ScalarQueryParameter(
                                "book", "STRING", row["book"]
                            ),
                            bigquery.ScalarQueryParameter(
                                "token", "STRING", t["expo_push_token"]
                            ),
                        ]
                    ),
                )

            except Exception as e:
                print("❌ Bad line push failed:", e)
                failures += 1

    return {
        "ok": True,
        "bad_lines": len(bad_lines),
        "tokens": len(tokens),
        "sent": sent,
        "failures": failures,
    }
