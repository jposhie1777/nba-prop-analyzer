import os
import requests

API_KEY = os.getenv("BALLDONTLIE_API_KEY")

def get(url: str, params: dict):
    resp = requests.get(
        url,
        headers={"Authorization": API_KEY},
        params=params,
        timeout=10,   # ⬅️ lower
    )
    return resp
