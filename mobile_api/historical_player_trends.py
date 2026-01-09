from fastapi import APIRouter
from google.cloud import bigquery
from typing import List, Dict, Any
from fastapi.encoders import jsonable_encoder

router = APIRouter(
    prefix="/historical",
    tags=["historical"]
)

bq = bigquery.Client()

QUERY = """
SELECT
  player,

  pts_last5_list, pts_last10_list, pts_last20_list,
  reb_last5_list, reb_last10_list, reb_last20_list,
  ast_last5_list, ast_last10_list, ast_last20_list,
  stl_last5_list, stl_last10_list, stl_last20_list,
  blk_last5_list, blk_last10_list, blk_last20_list,

  pra_last5_list, pra_last10_list, pra_last20_list,
  pr_last5_list,  pr_last10_list,  pr_last20_list,
  pa_last5_list,  pa_last10_list,  pa_last20_list,
  ra_last5_list,  ra_last10_list,  ra_last20_list,

  fgm_last5_list, fgm_last10_list, fgm_last20_list,
  fga_last5_list, fga_last10_list, fga_last20_list,

  fg3m_last5_list, fg3m_last10_list, fg3m_last20_list,
  fg3a_last5_list, fg3a_last10_list, fg3a_last20_list,

  ftm_last5_list, ftm_last10_list, ftm_last20_list,
  fta_last5_list, fta_last10_list, fta_last20_list,

  turnover_last5_list, turnover_last10_list, turnover_last20_list,
  pf_last5_list, pf_last10_list, pf_last20_list,

  last5_dates, last10_dates, last20_dates
FROM `graphite-flare-477419-h7.nba_live.historical_player_trends`
"""

@router.get("/player-trends")
def get_player_trends():
    rows = bq.query(QUERY).result()
    data = [dict(row) for row in rows]
    return jsonable_encoder(data)