import requests

EXPO_PUSH_URL = "https://exp.host/--/api/v2/push/send"

def send_push(token: str, title: str, body: str, data=None):
    payload = {
        "to": token,
        "sound": "default",
        "title": title,
        "body": body,
        "data": data or {},
    }

    r = requests.post(EXPO_PUSH_URL, json=payload, timeout=5)
    r.raise_for_status()