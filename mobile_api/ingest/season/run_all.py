# ingest/season/run_all.py

from google.cloud import bigquery

from mobile_api.ingest.common.bq import get_bq_client
from mobile_api.ingest.common.logging import now_ts

from mobile_api.ingest.season.player.general import run as run_general
from mobile_api.ingest.season.player.clutch import run as run_clutch
from mobile_api.ingest.season.player.shooting import run as run_shooting
from mobile_api.ingest.season.player.defense import run as run_defense


BATCH_SIZE = 25  


def chunked(lst, size):
    for i in range(0, len(lst), size):
        yield lst[i : i + size]


def run_all(season: int, season_type: str = "regular"):
    print(f"🏀 ingesting season={season} type={season_type}")

    bq = get_bq_client()

    # --------------------------------------------
    # Fetch canonical active players
    # --------------------------------------------
    job = bq.query(
        """
        SELECT DISTINCT player_id
        FROM `nba_goat_data.active_players`
        WHERE season = @season
        ORDER BY player_id
        """,
        job_config=bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("season", "INT64", season)
            ]
        ),
    )

    player_ids = [r.player_id for r in job]

    if not player_ids:
        print("⚠️ no active players found")
        return

    print(f"👥 total active players: {len(player_ids)}")

    batches = list(chunked(player_ids, BATCH_SIZE))

    # --------------------------------------------
    # Run batches
    # --------------------------------------------
    for idx, batch in enumerate(batches, start=1):
        run_ts = now_ts()
        print(
            f"🚀 batch {idx}/{len(batches)} "
            f"({len(batch)} players) run_ts={run_ts}"
        )

        run_general(season, season_type, batch, run_ts)
        run_clutch(season, season_type, batch, run_ts)
        run_shooting(season, season_type, batch, run_ts)
        run_defense(season, season_type, batch, run_ts)

    print("✅ season ingestion complete")
