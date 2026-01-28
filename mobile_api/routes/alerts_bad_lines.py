from fastapi import APIRouter
from bq import get_bq_client
from push import send_push   # ✅ root-level import

router = APIRouter(prefix="/alerts", tags=["alerts"])

bq = get_bq_client()


@router.post("/bad-lines/check")
def check_bad_line_alerts(min_score: float = 1.25):
    bad_lines_job = bq.query(
        """
        SELECT
          prop_id,
          player_name,
          market,
          line_value,
          odds,
          bad_line_score
        FROM nba_live.v_bad_line_alerts_scored
        WHERE is_bad_line = TRUE
          AND bad_line_score >= @min_score
        """,
        job_config={
            "query_parameters": [
                {
                    "name": "min_score",
                    "parameterType": {"type": "FLOAT64"},
                    "parameterValue": {"value": min_score},
                }
            ]
        },
    )

    bad_lines = list(bad_lines_job.result())

    if not bad_lines:
        return {"sent": 0}

    tokens_job = bq.query(
        "SELECT expo_push_token FROM nba_live.push_tokens"
    )
    tokens = [r["expo_push_token"] for r in tokens_job.result()]

    sent = 0

    for row in bad_lines:
        title = "⚠️ Bad Line Detected"
        body = (
            f"{row['player_name']} "
            f"{row['market']} {row['line_value']} "
            f"({row['odds']:+})"
        )

        for token in tokens:
            send_push(
                token=token,
                title=title,
                body=body,
                data={
                    "prop_id": row["prop_id"],
                    "bad_line_score": row["bad_line_score"],
                },
            )
            sent += 1

    return {
        "bad_lines": len(bad_lines),
        "tokens": len(tokens),
        "sent": sent,
    }