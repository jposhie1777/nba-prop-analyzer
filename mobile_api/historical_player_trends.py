from fastapi import APIRouter
from fastapi.responses import JSONResponse
from google.cloud import bigquery
from datetime import date, datetime

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

        for k, v in r.items():
            # REPEATED fields
            if isinstance(v, (list, tuple)):
                r[k] = [
                    x.isoformat() if isinstance(x, (date, datetime)) else x
                    for x in v
                ]

            # Scalar DATE / DATETIME (just in case)
            elif isinstance(v, (date, datetime)):
                r[k] = v.isoformat()

        out.append(r)

    return JSONResponse(content=out)