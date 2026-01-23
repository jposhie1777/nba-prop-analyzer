from fastapi import APIRouter, Query
from typing import Optional, List
from google.cloud import bigquery
import os

router = APIRouter(
    prefix="/props",
    tags=["props"],
)

PROJECT = os.getenv("GCP_PROJECT") or os.getenv("GOOGLE_CLOUD_PROJECT")
DATASET = "nba_live"
VIEW = "v_player_prop_odds_master"

def get_bq():
    return bigquery.Client(project=PROJECT)