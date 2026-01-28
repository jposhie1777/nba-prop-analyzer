from fastapi import APIRouter
from bq import get_bq_client

router = APIRouter(prefix="/push", tags=["push"])

@router.post("/register")
def register_push_token(
    user_id: str,
    expo_push_token: str,
):
    bq.query("""
      MERGE nba_live.push_tokens t
      USING (SELECT @user_id AS user_id, @token AS token) s
      ON t.user_id = s.user_id AND t.expo_push_token = s.token
      WHEN NOT MATCHED THEN
        INSERT (user_id, expo_push_token)
        VALUES (s.user_id, s.token)
    """, {
        "user_id": user_id,
        "token": expo_push_token,
    })

    return {"ok": True}