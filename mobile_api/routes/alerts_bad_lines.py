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

    # ----------------------------------
    # 1. Fetch NEW bad lines (deduped)
    # ----------------------------------
    bad_lines = list(
        bq.query(
            """
            SELECT *
            FROM nba_live.v_bad_line_alerts_scored bl
            WHERE bl.is_bad_line = TRUE
            AND bl.bad_line_score >= @min_score
            AND NOT EXISTS (
                SELECT 1
                FROM nba_live.bad_line_alert_log l
                WHERE CAST(l.prop_id AS STRING) = CAST(bl.prop_id AS STRING)
            )
            ORDER BY bl.bad_line_score DESC
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

    # ----------------------------------
    # 2. Fetch push tokens
    # ----------------------------------
    tokens = list(
        bq.query(
            "SELECT expo_push_token FROM nba_live.push_tokens"
        )
    )

    sent = 0
    failures = 0

    # ----------------------------------
    # 3. Send pushes
    # ----------------------------------
    for row in bad_lines:
        for t in tokens:
            try:
                send_push(
                    token=t["expo_push_token"],
                    title="⚠️ Bad Line Detected",
                    body=(
                        f"{row['player_name']} "
                        f"{row['market']} {row['line_value']} "
                        f"({row['odds']:+})"
                    ),
                    data={
                        "prop_id": row["prop_id"],
                        "type": "bad_line",
                    },
                )

                sent += 1

                # Log success (dedupe)
                bq.query(
                    """
                    INSERT INTO nba_live.bad_line_alert_log (prop_id, expo_push_token)
                    VALUES (@prop_id, @token)
                    """,
                    job_config=bigquery.QueryJobConfig(
                        query_parameters=[
                            bigquery.ScalarQueryParameter(
                                "prop_id", "STRING", row["prop_id"]
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
