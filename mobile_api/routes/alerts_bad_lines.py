from fastapi import APIRouter
from google.cloud import bigquery

from bq import get_bq_client
from routes.push import send_push   # ✅ CORRECT

router = APIRouter(prefix="/alerts", tags=["alerts"])


@router.post("/bad-lines/check")
def check_bad_line_alerts(min_score: float = 1.25):
    bq = get_bq_client()

    bad_lines = list(
        bq.query(
            """
            SELECT *
            FROM nba_live.v_bad_line_alerts_scored
            WHERE is_bad_line = TRUE
              AND bad_line_score >= @min_score
            """,
            job_config=bigquery.QueryJobConfig(
                query_parameters=[
                    bigquery.ScalarQueryParameter(
                        "min_score", "FLOAT", min_score
                    )
                ]
            ),
        )
    )

    tokens = list(
        bq.query("SELECT expo_push_token FROM nba_live.push_tokens")
    )

    sent = 0

    for row in bad_lines:
        for t in tokens:
            send_push(
                token=t["expo_push_token"],
                title="⚠️ Bad Line Detected",
                body=f"{row['player_name']} {row['market']} {row['line_value']} ({row['odds']:+})",
                data={"prop_id": row["prop_id"]},
            )
            sent += 1

    return {
        "bad_lines": len(bad_lines),
        "tokens": len(tokens),
        "sent": sent,
    }