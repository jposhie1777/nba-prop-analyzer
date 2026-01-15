from typing import List
from google.cloud import bigquery
from mobile_api.ingest.common.bq import get_bq_client

def get_active_player_ids(season: int = 2025) -> list[int]:
    bq = get_bq_client()

    query = """
    SELECT DISTINCT
      CAST(JSON_VALUE(payload, '$.player.id') AS INT64) AS player_id
    FROM `nba_goat_data.player_season_averages_raw`
    WHERE season = @season
      AND JSON_VALUE(payload, '$.player.id') IS NOT NULL
    """

    job = bq.query(
        query,
        job_config={
            "query_parameters": [
                {
                    "name": "season",
                    "parameterType": {"type": "INT64"},
                    "parameterValue": {"value": season},
                }
            ]
        },
    )

    return [row.player_id for row in job.result()]