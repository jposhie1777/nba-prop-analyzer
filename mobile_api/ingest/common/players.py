from typing import List
from google.cloud import bigquery
from mobile_api.ingest.common.bq import get_bq_client

def get_active_player_ids(limit: int = 500) -> List[int]:
    bq = get_bq_client()

    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("limit", "INT64", limit)
        ]
    )

    query = """
    SELECT DISTINCT
      COALESCE(player_id, id) AS player_id
    FROM `nba_goat_data.player_lookup`
    WHERE COALESCE(player_id, id) IS NOT NULL
    LIMIT @limit
    """

    rows = bq.query(query, job_config=job_config).result()
    return [r.player_id for r in rows]