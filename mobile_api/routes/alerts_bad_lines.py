from fastapi import APIRouter
from bq import get_bq_client
from lib.push import send_push

router = APIRouter(prefix="/alerts", tags=["alerts"])

@router.post("/bad-lines/check")
def check_bad_line_alerts():
    rows = bq.query("""
      SELECT *
      FROM nba_live.v_bad_line_alerts_scored
      WHERE is_bad_line = TRUE
        AND bad_line_score >= 1.25
    """)

    tokens = bq.query("""
      SELECT expo_push_token
      FROM nba_live.push_tokens
    """)

    for row in rows:
        title = "⚠️ Bad Line Detected"
        body = f"{row['player_name']} {row['market']} {row['line_value']} ({row['odds']:+})"

        for t in tokens:
            send_push(
              token=t["expo_push_token"],
              title=title,
              body=body,
              data={"prop_id": row["prop_id"]},
            )

    return {"sent": len(rows)}