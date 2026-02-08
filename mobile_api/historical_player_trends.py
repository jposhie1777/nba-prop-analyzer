from fastapi import APIRouter
from fastapi.responses import JSONResponse
from datetime import date, datetime

from bq import get_bq_client

router = APIRouter(
    prefix="/historical",
    tags=["historical"]
)

QUERY = """
SELECT *
FROM `graphite-flare-477419-h7.nba_goat_data.historical_player_trends`
"""

@router.get("/player-trends")
def get_player_trends():
    bq = get_bq_client()
    rows = bq.query(QUERY).result()

    out = []

    for row in rows:
        r = {}

        for k, v in dict(row).items():

            # 🔴 Convert DATE / DATETIME
            if isinstance(v, (date, datetime)):
                r[k] = v.isoformat()

            # 🔴 Convert REPEATED fields
            elif isinstance(v, (list, tuple)):
                r[k] = [
                    x.isoformat() if isinstance(x, (date, datetime)) else x
                    for x in v
                ]

            else:
                r[k] = v

        out.append(r)

    return JSONResponse(content=out)