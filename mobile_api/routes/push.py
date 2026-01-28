from fastapi import APIRouter
from pydantic import BaseModel
from google.cloud import bigquery

from bq import get_bq_client

router = APIRouter(prefix="/push", tags=["push"])

bq = get_bq_client()


class PushRegisterRequest(BaseModel):
    user_id: str
    expo_push_token: str


@router.post("/register")
def register_push_token(payload: PushRegisterRequest):
    query = """
    MERGE nba_live.push_tokens t
    USING (
      SELECT @user_id AS user_id, @token AS expo_push_token
    ) s
    ON t.user_id = s.user_id AND t.expo_push_token = s.expo_push_token
    WHEN NOT MATCHED THEN
      INSERT (user_id, expo_push_token)
      VALUES (s.user_id, s.expo_push_token)
    """

    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter(
                "user_id", "STRING", payload.user_id
            ),
            bigquery.ScalarQueryParameter(
                "token", "STRING", payload.expo_push_token
            ),
        ]
    )

    bq.query(query, job_config=job_config).result()

    return {"ok": True}