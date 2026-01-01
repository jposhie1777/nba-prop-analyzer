# goat_auth.py
import os
import requests
from google.oauth2 import service_account
from google.auth.transport.requests import Request

GOAT_AUDIENCE = "https://goat-ingestion-763243624328.us-central1.run.app"


def get_goat_id_token():
    key_path = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")
    if not key_path:
        raise RuntimeError("GOOGLE_APPLICATION_CREDENTIALS not set")

    creds = service_account.IDTokenCredentials.from_service_account_file(
        key_path,
        target_audience=GOAT_AUDIENCE,
    )

    creds.refresh(Request())
    return creds.token
    
def call_goat(url: str, params: dict | None = None):
    token = get_goat_id_token()
    headers = {"Authorization": f"Bearer {token}"}

    r = requests.get(
        url,
        headers=headers,
        params=params or {},
        timeout=180,
    )
    r.raise_for_status()

    return r.json() if r.text else {"status": "ok"}