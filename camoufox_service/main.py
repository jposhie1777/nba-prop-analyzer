"""
Camoufox Browser Proxy Service for Cloud Run.

Accepts HTTP POST /fetch requests with a target URL and optional capture
patterns, then returns the rendered page body plus any captured XHR/fetch
responses that matched the patterns.

This service is used to bypass SSL/TLS fingerprinting by FanDuel and
DraftKings sportsbook APIs that block direct calls from GCP/GitHub Actions.

POST /fetch
  {
    "url": "https://sportsbook.fanduel.com/soccer/premier-league",
    "headers": {},                          # optional extra request headers
    "wait_for_selector": ".market-row",     # optional CSS selector to await
    "capture_patterns": ["sbapi", "api.fanduel.com"],  # XHR URL substrings
    "wait_ms": 5000,                        # post-load wait for JS hydration
    "timeout_ms": 30000,                    # navigation + selector timeout
    "prime_url": "https://sportsbook.fanduel.com"  # optional cookie-prime URL
  }

Response:
  {
    "status": 200,
    "body": "<html>...</html>",
    "headers": {},
    "captured_requests": [
      {"url": "...", "status": 200, "body": {...}}
    ]
  }
"""

from __future__ import annotations

import json
import logging
import os
from typing import Any, Dict, List, Optional

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = FastAPI(title="Camoufox Browser Proxy")


# ---------------------------------------------------------------------------
# Request / Response models
# ---------------------------------------------------------------------------

class FetchRequest(BaseModel):
    url: str
    headers: Dict[str, str] = {}
    wait_for_selector: Optional[str] = None
    capture_patterns: List[str] = []
    wait_ms: int = 5000
    timeout_ms: int = 30000
    prime_url: Optional[str] = None


class CapturedRequest(BaseModel):
    url: str
    status: int
    body: Any
    request_headers: Dict[str, str] = {}


class FetchResponse(BaseModel):
    status: int
    body: str
    headers: Dict[str, str] = {}
    captured_requests: List[CapturedRequest] = []


# ---------------------------------------------------------------------------
# Endpoints
# ---------------------------------------------------------------------------

@app.get("/health")
def health():
    return {"status": "ok"}


@app.post("/fetch", response_model=FetchResponse)
def fetch_url(req: FetchRequest) -> FetchResponse:
    """
    Use Camoufox (Firefox with real TLS fingerprint) to load a URL, optionally
    capturing matching XHR/fetch responses, and return the result.

    This is a synchronous endpoint; FastAPI automatically runs sync def
    endpoints in a thread-pool worker so the event loop is not blocked.
    """
    from camoufox.sync_api import Camoufox

    logger.info(
        "fetch url=%s capture_patterns=%s wait_ms=%d",
        req.url,
        req.capture_patterns,
        req.wait_ms,
    )

    captured: List[CapturedRequest] = []

    try:
        with Camoufox(headless=True, geoip=True) as browser:
            context = browser.new_context(
                locale="en-US",
                viewport={"width": 1920, "height": 1080},
                extra_http_headers={
                    "Accept-Language": "en-US,en;q=0.9",
                    **req.headers,
                },
            )
            page = context.new_page()

            # Register XHR / fetch response interceptor
            if req.capture_patterns:
                def on_response(response: Any) -> None:
                    resp_url: str = response.url
                    if not any(p in resp_url for p in req.capture_patterns):
                        return
                    try:
                        ct = response.headers.get("content-type", "")
                        if "json" in ct or resp_url.lower().endswith(".json"):
                            body: Any = response.json()
                        else:
                            raw = response.text()
                            try:
                                body = json.loads(raw)
                            except Exception:
                                body = raw
                        # Capture request headers (needed for x-px-context token)
                        try:
                            req_hdrs = dict(response.request.headers)
                        except Exception:
                            req_hdrs = {}
                        captured.append(
                            CapturedRequest(
                                url=resp_url,
                                status=response.status,
                                body=body,
                                request_headers=req_hdrs,
                            )
                        )
                        logger.info("captured %s  status=%d", resp_url[:120], response.status)
                    except Exception as exc:
                        logger.warning("capture error for %s: %s", resp_url[:120], exc)

                page.on("response", on_response)

            # Prime with a lightweight homepage visit to establish cookies /
            # Cloudflare cf_clearance before hitting the real target page.
            if req.prime_url:
                logger.info("priming session via %s", req.prime_url)
                page.goto(
                    req.prime_url,
                    wait_until="domcontentloaded",
                    timeout=req.timeout_ms,
                )
                page.wait_for_timeout(2000)

            # Navigate to the target URL
            logger.info("navigating to %s", req.url)
            nav_response = page.goto(
                req.url,
                wait_until="domcontentloaded",
                timeout=req.timeout_ms,
            )

            # Wait for an optional CSS selector (e.g. a market row)
            if req.wait_for_selector:
                try:
                    page.wait_for_selector(
                        req.wait_for_selector, timeout=req.timeout_ms
                    )
                except Exception as exc:
                    logger.warning("wait_for_selector timed out: %s", exc)

            # Extra dwell time for JS hydration and deferred API calls
            if req.wait_ms > 0:
                page.wait_for_timeout(req.wait_ms)

            page_status = nav_response.status if nav_response else 200
            body_html = page.content()
            resp_headers: Dict[str, str] = (
                dict(nav_response.headers) if nav_response else {}
            )

            logger.info(
                "done url=%s status=%d captured=%d",
                req.url,
                page_status,
                len(captured),
            )

            return FetchResponse(
                status=page_status,
                body=body_html,
                headers=resp_headers,
                captured_requests=captured,
            )

    except Exception as exc:
        logger.exception("unhandled error fetching %s: %s", req.url, exc)
        raise HTTPException(status_code=500, detail=str(exc))
