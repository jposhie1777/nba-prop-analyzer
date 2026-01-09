from fastapi import APIRouter
from fastapi.responses import JSONResponse
from google.cloud import bigquery

router = APIRouter(
    prefix="/historical",
    tags=["historical"]
)

bq = bigquery.Client()

QUERY = """
SELECT *
FROM `graphite-flare-477419-h7.nba_goat_data.historical_player_trends`
"""

@router.get("/player-trends")
def get_player_trends():
    rows = bq.query(QUERY).result()

    out = []
    for row in rows:
        r = dict(row)

        # 🔴 CRITICAL: convert BigQuery REPEATED fields
        for k, v in r.items():
            if isinstance(v, (list, tuple)):
                r[k] = list(v)

        out.append(r)

    return JSONResponse(content=out)