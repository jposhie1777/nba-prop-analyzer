from fastapi import APIRouter
from google.cloud import bigquery
import requests
import os

from bq import get_bq_client

router = APIRouter(prefix="/push", tags=["push"])

EXPO_PUSH_URL = "https://exp.host/--/api/v2/push/send"


# --------------------------------------------------
# REGISTER DEVICE TOKEN
# --------------------------------------------------
@router.post("/register")
def register_push_token(user_id: str, expo_push_token: str):
    bq = get_bq_client()

    query = """
    MERGE nba_live.push_tokens t
    USING (SELECT @user_id AS user_id, @token AS token) s
    ON t.user_id = s.user_id AND t.expo_push_token = s.token
    WHEN NOT MATCHED THEN
      INSERT (user_id, expo_push_token)
      VALUES (s.user_id, s.token)
    """

    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("user_id", "STRING", user_id),
            bigquery.ScalarQueryParameter("token", "STRING", expo_push_token),
        ]
    )

    bq.query(query, job_config=job_config).result()

    return {"ok": True}


# --------------------------------------------------
# SEND PUSH (USED BY ALERTS)
# --------------------------------------------------
def send_push(token: str, title: str, body: str, data: dict | None = None):
    payload = {
        "to": token,
        "sound": "default",
        "title": title,
        "body": body,
        "data": data or {},
    }

    res = requests.post(EXPO_PUSH_URL, json=payload, timeout=10)
    res.raise_for_status()