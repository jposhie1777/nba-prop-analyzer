from typing import List
from mobile_api.ingest.common.bq import get_bq_client

def get_active_player_ids(limit: int = 500) -> List[int]:
    bq = get_bq_client()
    rows = bq.query("""
        SELECT DISTINCT player_id
        FROM `nba_goat_data.player_lookup`
        WHERE player_id IS NOT NULL
        LIMIT @limit
    """, job_config={
        "query_parameters": [
            {
                "name": "limit",
                "parameterType": {"type": "INT64"},
                "parameterValue": {"value": limit},
            }
        ]
    }).result()

    return [r.player_id for r in rows]