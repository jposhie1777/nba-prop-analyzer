from datetime import datetime
from google.cloud import bigquery
from bq import get_bq_client
from ingest.injuries.wowy import get_wowy_for_injured_players

STATS = ["pts", "reb", "ast", "fg3m"]

def refresh_wowy_cache_for_season(season: int) -> int:
    client = get_bq_client()

    # Clear old cache for season
    client.query(
        """
        DELETE FROM `nba_live.wowy_injured_cache`
        WHERE season = @season
        """,
        job_config=bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("season", "INT64", season)
            ]
        ),
    ).result()

    rows_written = 0

    for stat in STATS:
        wowy_results = get_wowy_for_injured_players(
            season=season,
            only_today_games=False,  # IMPORTANT: full season cache
        )

        rows = []
        for w in wowy_results:
            rows.append({
                "season": season,
                "stat": stat,
                "injured_player": w["injured_player"],
                "team_impact": w["team_impact"],
                "teammates": w["teammates"],
                "updated_at": datetime.utcnow().isoformat(),
            })

        if rows:
            errors = client.insert_rows_json(
                "nba_live.wowy_injured_cache",
                rows,
            )
            if errors:
                raise RuntimeError(errors)

            rows_written += len(rows)

    return rows_written
