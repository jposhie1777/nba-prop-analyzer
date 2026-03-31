"""
Shared Camoufox proxy client — auto-discovers Cloud Run URL and generates
GCP identity tokens so scrapers work hands-off from any environment.

URL resolution:
  1. CAMOUFOX_SERVICE_URL env var (explicit override / GitHub Actions)
  2. Auto-discover via `gcloud run services list` using active GCP credentials

Token resolution:
  1. CAMOUFOX_TOKEN env var (explicit override / GitHub Actions)
  2. Auto-generate from GOOGLE_APPLICATION_CREDENTIALS service account key
"""
from __future__ import annotations

import logging
import os
import subprocess
import time
from typing import Any, Dict, Optional

import requests

logger = logging.getLogger(__name__)

# Caches (URL rotates daily so cache per-session, token refreshes at ~50min)
_cached_service_url: Optional[str] = None
_cached_id_token: Optional[str] = None
_cached_id_token_expiry: float = 0

CLOUD_RUN_SERVICE_NAME = "camoufox-proxy"
GCP_PROJECT = "graphite-flare-477419-h7"


def _discover_cloud_run_url() -> str:
    """Find the current Cloud Run URL via gcloud CLI."""
    try:
        result = subprocess.run(
            [
                "gcloud", "run", "services", "list",
                f"--project={GCP_PROJECT}",
                f"--filter=metadata.name={CLOUD_RUN_SERVICE_NAME}",
                "--format=value(status.url)",
                "--limit=1",
            ],
            capture_output=True, text=True, timeout=15,
        )
        url = result.stdout.strip()
        if url and url.startswith("https://"):
            logger.info("Auto-discovered Camoufox URL: %s", url)
            return url
        logger.warning("gcloud returned no URL for %s: stdout=%r stderr=%r",
                       CLOUD_RUN_SERVICE_NAME, result.stdout[:200], result.stderr[:200])
    except FileNotFoundError:
        logger.warning("gcloud CLI not found — cannot auto-discover Camoufox URL")
    except Exception as exc:
        logger.warning("Failed to auto-discover Camoufox URL: %s", exc)
    return ""


def get_camoufox_url() -> str:
    global _cached_service_url

    # Explicit env var always wins
    url = os.environ.get("CAMOUFOX_SERVICE_URL", "").rstrip("/")
    if url:
        return url

    # Return session cache (URL rotates daily, stable within a run)
    if _cached_service_url:
        return _cached_service_url

    # Auto-discover from Cloud Run
    url = _discover_cloud_run_url().rstrip("/")
    if url:
        _cached_service_url = url
        return url

    raise RuntimeError(
        "CAMOUFOX_SERVICE_URL not set and auto-discovery failed. "
        "Set the env var or ensure gcloud is authenticated."
    )


def get_camoufox_token() -> str:
    """
    Return a GCP identity token for the Cloud Run service.

    Priority:
      1. CAMOUFOX_TOKEN env var (explicit override / GitHub Actions)
      2. Auto-generate from GOOGLE_APPLICATION_CREDENTIALS service account key
    """
    global _cached_id_token, _cached_id_token_expiry

    # Explicit token always wins
    explicit = os.environ.get("CAMOUFOX_TOKEN", "")
    if explicit:
        return explicit

    # Return cached token if still fresh (tokens last ~1h, refresh at 50min)
    if _cached_id_token and time.time() < _cached_id_token_expiry:
        return _cached_id_token

    # Auto-generate from service account credentials
    target_audience = get_camoufox_url()
    try:
        from google.oauth2 import service_account as _sa
        from google.auth.transport.requests import Request as _AuthRequest

        creds_path = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS", "")
        if not creds_path:
            logger.warning("No GOOGLE_APPLICATION_CREDENTIALS — cannot auto-generate identity token")
            return ""

        creds = _sa.IDTokenCredentials.from_service_account_file(
            creds_path, target_audience=target_audience,
        )
        creds.refresh(_AuthRequest())
        _cached_id_token = creds.token
        # Refresh 10 minutes before expiry (tokens typically last 1 hour)
        _cached_id_token_expiry = time.time() + 3000
        logger.info("Auto-generated GCP identity token for %s", target_audience)
        return _cached_id_token or ""
    except Exception as exc:
        logger.warning("Failed to auto-generate identity token: %s", exc)
        return ""


def call_proxy(payload: Dict[str, Any], timeout: int = 150) -> Dict[str, Any]:
    """POST to the Camoufox /fetch endpoint with auto-auth."""
    service_url = get_camoufox_url()
    token = get_camoufox_token()
    headers = {"Content-Type": "application/json"}
    if token:
        headers["Authorization"] = f"Bearer {token}"
    resp = requests.post(
        f"{service_url}/fetch",
        json=payload,
        headers=headers,
        timeout=timeout,
    )
    resp.raise_for_status()
    return resp.json()
