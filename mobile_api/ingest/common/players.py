from typing import List
from google.cloud import bigquery
from mobile_api.ingest.common.bq import get_bq_client

def get_active_player_ids(limit: int = 500) -> List[int]:
    bq = get_bq_client()

    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter(
                "limit", "INT64", limit
            )
        ]
    )

    rows = bq.query(
        """
        SELECT DISTINCT player_id
        FROM `nba_goat_data.player_lookup`
        WHERE player_id IS NOT NULL
        LIMIT @limit
        """,
        job_config=job_config,
    ).result()

    return [r.player_id for r in rows]