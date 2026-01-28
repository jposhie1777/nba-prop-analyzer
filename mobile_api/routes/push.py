# push.py
from fastapi import APIRouter, Request
from pydantic import BaseModel
import requests
from google.cloud import bigquery
import json
from typing import Optional
from bq import get_bq_client

router = APIRouter(prefix="/push", tags=["push"])

EXPO_PUSH_URL = "https://exp.host/--/api/v2/push/send"


# ============================
#   PUSH TOKEN REGISTRATION
# ============================

class PushRegisterBody(BaseModel):
    user_id: str
    expo_push_token: str


@router.post("/register")
async def register_push_token(request: Request, body: PushRegisterBody):
    # 🔍 DEBUG: confirm request actually reaches FastAPI
    raw_body = await request.body()
    print("📥 [PUSH] Raw body:", raw_body)
    print("📥 [PUSH] Headers:", dict(request.headers))

    print("📥 [PUSH] Parsed body:", body.dict())

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
            bigquery.ScalarQueryParameter(
                "user_id", "STRING", body.user_id
            ),
            bigquery.ScalarQueryParameter(
                "token", "STRING", body.expo_push_token
            ),
        ]
    )

    bq.query(query, job_config=job_config).result()

    print("✅ [PUSH] Token registered in BigQuery")

    return {"ok": True}


# ============================
#   SEND PUSH
# ============================

def send_push(
    token: str,
    title: str,
    body: str,
    data: Optional[dict] = None,
):
    payload = {
        "to": token,
        "sound": "default",
        "title": title,
        "body": body,
        "data": data or {},
    }

    print("🚀 [PUSH] Sending Expo push")
    print("➡️  Token:", token)
    print("➡️  Title:", title)
    print("➡️  Body:", body)
    print("➡️  Data:", json.dumps(payload["data"], default=str))

    try:
        resp = requests.post(
            EXPO_PUSH_URL,
            json=payload,
            timeout=10,
        )
    except Exception as e:
        print("❌ [PUSH] Network error sending to Expo:", e)
        raise

    print("📡 [PUSH] Expo HTTP status:", resp.status_code)

    if not resp.ok:
        print("❌ [PUSH] Expo HTTP error response:")
        print(resp.text)
        resp.raise_for_status()

    try:
        result = resp.json()
    except Exception as e:
        print("❌ [PUSH] Failed to parse Expo JSON response:", e)
        print("Raw body:", resp.text)
        raise

    print("📬 [PUSH] Expo response JSON:", json.dumps(result, indent=2))

    data_items = result.get("data")

    if not isinstance(data_items, list):
        raise RuntimeError(
            f"Unexpected Expo response shape (data is not list): {result}"
        )

    errors = []

    for idx, item in enumerate(data_items):
        print(f"🔎 [PUSH] Result[{idx}]:", item)

        status = item.get("status")

        if status != "ok":
            print("❌ [PUSH] Expo rejected message:", item)
            errors.append(item)
        else:
            print("✅ [PUSH] Expo accepted message:", item)

    if errors:
        raise RuntimeError(f"Expo push rejected one or more messages: {errors}")

    print("🎉 [PUSH] Push completed successfully")
    return result
