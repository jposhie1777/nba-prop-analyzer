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
    cooldown_minutes: int = 30,
):
    """
    Sends push alerts for newly detected bad lines.

    Logical dedupe key:
      (player_id, market, line_key)

    where:
      line_key = CAST(line_value * 2 AS INT64)

    Enforces cooldown window to prevent spam.
    """

    bq = get_bq_client()

    # ----------------------------------
    # 1. Fetch NEW bad lines (DEDUPED + SAFE)
    # ----------------------------------
    bad_lines = list(
        bq.query(
            """
            WITH normalized AS (
              SELECT
                bl.*,
                CAST(ROUND(bl.line_value * 2) AS INT64) AS line_key,
                ROUND(bl.line_value, 1) AS display_line
              FROM nba_live.v_bad_line_alerts_scored bl
              WHERE bl.is_bad_line = TRUE
                AND bl.bad_line_score >= @min_score
            ),

            ranked AS (
              SELECT
                n.*,
                ROW_NUMBER() OVER (
                  PARTITION BY
                    n.player_id,
                    n.market,
                    n.line_key
                  ORDER BY n.bad_line_score DESC
                ) AS rn
              FROM normalized n
              WHERE NOT EXISTS (
                SELECT 1
                FROM nba_live.bad_line_alert_log l
                WHERE
                  l.player_id = n.player_id
                  AND l.market = n.market
                  AND l.line_key = n.line_key
                  AND l.alerted_at >
                    TIMESTAMP_SUB(
                      CURRENT_TIMESTAMP(),
                      INTERVAL @cooldown_minutes MINUTE
                    )
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
                        "min_score", "FLOAT64", min_score
                    ),
                    bigquery.ScalarQueryParameter(
                        "max_lines", "INT64", max_lines
                    ),
                    bigquery.ScalarQueryParameter(
                        "cooldown_minutes", "INT64", cooldown_minutes
                    ),
                ]
            ),
        )
    )

    if not bad_lines:
        return {
            "ok": True,
            "sent": 0,
            "reason": "no new bad lines (deduped + cooldown)",
        }

    # ----------------------------------
    # 2. Fetch push tokens
    # ----------------------------------
    tokens = list(
        bq.query(
            """
            SELECT DISTINCT expo_push_token
            FROM nba_live.push_tokens
            WHERE expo_push_token IS NOT NULL
            """
        )
    )

    if not tokens:
        return {
            "ok": True,
            "sent": 0,
            "reason": "no push tokens",
        }

    sent = 0
    failures = 0

    # ----------------------------------
    # 3. Send pushes + log alerts
    # ----------------------------------
    for row in bad_lines:
        for t in tokens:
            try:
                send_push(
                    token=t["expo_push_token"],
                    title="⚠️ Bad Line Detected",
                    body=(
                        f"{row['player_name']} "
                        f"{row['market']} {row['display_line']} "
                        f"({row['odds']:+})"
                    ),
                    data={
                        "type": "bad_line",
                        "player_id": row["player_id"],
                        "market": row["market"],
                        "line": row["display_line"],
                    },
                )

                sent += 1

                # ----------------------------------
                # Log alert (INTEGER DEDUPE KEY)
                # ----------------------------------
                bq.query(
                    """
                    INSERT INTO nba_live.bad_line_alert_log (
                      player_id,
                      player_name,
                      market,
                      line_value,
                      line_key,
                      expo_push_token
                    )
                    VALUES (
                      @player_id,
                      @player_name,
                      @market,
                      @line_value,
                      @line_key,
                      @token
                    )
                    """,
                    job_config=bigquery.QueryJobConfig(
                        query_parameters=[
                            bigquery.ScalarQueryParameter(
                                "player_id", "INT64", row["player_id"]
                            ),
                            bigquery.ScalarQueryParameter(
                                "player_name", "STRING", row["player_name"]
                            ),
                            bigquery.ScalarQueryParameter(
                                "market", "STRING", row["market"]
                            ),
                            bigquery.ScalarQueryParameter(
                                "line_value", "FLOAT64", row["display_line"]
                            ),
                            bigquery.ScalarQueryParameter(
                                "line_key", "INT64", row["line_key"]
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
        "cooldown_minutes": cooldown_minutes,
    }
