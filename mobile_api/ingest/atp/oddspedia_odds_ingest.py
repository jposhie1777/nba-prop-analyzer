"""
Oddspedia scraping client.

Responsible only for:
    - loading the odds page
    - extracting match metadata
    - fetching market groups
    - returning normalized market rows

Output format returned to ingest script:

[
  {
    match metadata...
    market_rows: [...]
  }
]
"""

import asyncio
import json
import re
from typing import Dict, List, Any

import httpx
from playwright.sync_api import sync_playwright

BASE_URL = "https://oddspedia.com"

MARKET_GROUPS = [
    2,  # moneyline
    3,  # spreads
    4,  # totals
    7,  # correct score
]

HEADERS = {
    "accept": "application/json, text/plain, */*",
    "referer": "https://oddspedia.com/us/tennis",
    "user-agent": "Mozilla/5.0",
}


class OddspediaClient:

    def scrape(self, url: str) -> List[Dict[str, Any]]:
        matches = self._scrape_page(url)

        if not matches:
            return []

        asyncio.run(self._enrich_markets(matches))

        return matches


    # -----------------------------------------------------
    # PAGE SCRAPER
    # -----------------------------------------------------

    def _scrape_page(self, url: str) -> List[Dict]:

        with sync_playwright() as p:

            browser = p.chromium.launch(headless=True)
            page = browser.new_page()

            page.goto(url, timeout=60000)

            html = page.content()

            browser.close()

        nuxt_match = re.search(
            r"window\.__NUXT__=(\{.*?\});</script>",
            html,
            re.DOTALL,
        )

        if not nuxt_match:
            return []

        nuxt_data = json.loads(nuxt_match.group(1))

        matches = []

        events = (
            nuxt_data
            .get("data", [{}])[0]
            .get("events", [])
        )

        for event in events:

            matches.append(
                {
                    "match_id": event.get("id"),
                    "sport": "tennis",
                    "date_utc": event.get("start_time"),
                    "home_team": event.get("home_name"),
                    "away_team": event.get("away_name"),
                    "home_team_id": event.get("home_id"),
                    "away_team_id": event.get("away_id"),
                    "league_id": event.get("league_id"),
                    "inplay": event.get("inplay", False),
                    "market_rows": [],
                }
            )

        return matches


    # -----------------------------------------------------
    # MARKET FETCHER
    # -----------------------------------------------------

    async def _enrich_markets(self, matches):

        async with httpx.AsyncClient(headers=HEADERS, timeout=30) as client:

            tasks = []

            for match in matches:
                for group in MARKET_GROUPS:
                    tasks.append(
                        self._fetch_market_group(client, match, group)
                    )

            await asyncio.gather(*tasks)


    async def _fetch_market_group(
        self,
        client: httpx.AsyncClient,
        match: Dict,
        group_id: int,
    ):

        params = {
            "matchId": match["match_id"],
            "marketGroupId": group_id,
            "geoCode": "US",
            "geoState": "VA",
            "language": "us",
            "inplay": 0,
            "r": "si",
        }

        try:

            r = await client.get(
                f"{BASE_URL}/api/v1/getMatchMaxOddsByGroup",
                params=params,
            )

            if r.status_code != 200:
                return

            payload = r.json().get("data")

            if not payload:
                return

            self._parse_market_group(match, payload)

        except Exception:
            return


    # -----------------------------------------------------
    # PARSER
    # -----------------------------------------------------

    def _parse_market_group(self, match, payload):

        market_name = payload.get("market_name")
        market_group_id = payload.get("market_group_id")

        periods = payload.get("periods", [])

        period_map = {str(p["id"]): p["name"] for p in periods}

        odds_data = payload.get("odds", {})

        for period_id, period in odds_data.items():

            period_name = period_map.get(period_id)

            for outcome_key, outcome in period["odds"].items():

                odds_decimal = float(outcome.get("odds_value"))

                odds_american = self._decimal_to_american(odds_decimal)

                match["market_rows"].append(
                    {
                        "market_group_id": market_group_id,
                        "market_group_name": market_name,
                        "market": market_name.lower(),
                        "period_id": int(period_id),
                        "period_name": period_name,
                        "bookie_id": outcome.get("bid"),
                        "bookie": outcome.get("bookie_name"),
                        "bookie_slug": outcome.get("bookie_slug"),
                        "outcome_key": outcome_key,
                        "outcome_name": outcome_key,
                        "outcome_side": "home"
                        if outcome_key == "o1"
                        else "away",
                        "outcome_order": 1
                        if outcome_key == "o1"
                        else 2,
                        "odds_decimal": odds_decimal,
                        "odds_american": odds_american,
                        "odds_status": outcome.get("odds_status"),
                        "odds_direction": outcome.get("odds_direction"),
                        "line_value": None,
                        "home_handicap": None,
                        "away_handicap": None,
                        "handicap_label": None,
                        "winning_side": None,
                        "bet_link": outcome.get("odds_link"),
                        "market_json": payload,
                        "outcome_json": outcome,
                    }
                )


    # -----------------------------------------------------
    # UTILS
    # -----------------------------------------------------

    def _decimal_to_american(self, decimal):

        if decimal >= 2:
            return int((decimal - 1) * 100)

        return int(-100 / (decimal - 1))