“”“
ATP tournament configuration for Oddspedia scraping.

Provides the active ATP tournament URL, league slug, and season ID.
These are used by both the odds and match-info ingests.

The active tournament can be set via environment variables, which allows
GitHub Actions workflows to pass in the correct tournament per-run:

```
ODDSPEDIA_ATP_URL       Full Oddspedia tournament URL
ODDSPEDIA_ATP_SLUG      League slug (e.g. atp-miami)
ODDSPEDIA_ATP_SEASON_ID Season ID integer
```

If those are not set, falls back to a hardcoded known-active tournament list
and picks the one closest to today’s date (or currently ongoing).

Usage:
from mobile_api.ingest.atp.oddspedia_atp_config import get_atp_config
cfg = get_atp_config()
# cfg.url, cfg.league_slug, cfg.season_id, cfg.tournament_name
“””

from **future** import annotations

import os
from dataclasses import dataclass
from datetime import date, datetime, timezone
from typing import List, Optional

@dataclass
class ATPTournamentConfig:
tournament_name: str
url: str
league_slug: str
season_id: int
start_date: date
end_date: date

# ── Known 2026 ATP tournament calendar ────────────────────────────────────────

# Update this list as new tournaments are announced / season IDs are confirmed.

# season_id values come from Oddspedia’s getMatchInfo API (match_info.season_id).

# 

# To add a new tournament:

# 1. Run the scraper against the tournament URL with –scrape-only

# 2. Check the log for: “[scraper] match=XXXXX enriched: …”

# 3. Look at the match_info.season_id in the output JSON

# 

# URL pattern: https://oddspedia.com/us/a/tennis/{country}/{slug}

ATP_2026_TOURNAMENTS: List[ATPTournamentConfig] = [
ATPTournamentConfig(
tournament_name=“ATP Miami Open”,
url=“https://www.oddspedia.com/us/a/tennis/usa/atp-miami”,
league_slug=“atp-miami”,
season_id=134091,
start_date=date(2026, 3, 18),
end_date=date(2026, 3, 30),
),
ATPTournamentConfig(
tournament_name=“ATP Monte Carlo”,
url=“https://www.oddspedia.com/us/a/tennis/monaco/rolex-monte-carlo-masters”,
league_slug=“rolex-monte-carlo-masters”,
season_id=0,  # TODO: confirm season_id once available
start_date=date(2026, 4, 6),
end_date=date(2026, 4, 13),
),
ATPTournamentConfig(
tournament_name=“ATP Madrid Open”,
url=“https://www.oddspedia.com/us/a/tennis/spain/madrid-open”,
league_slug=“madrid-open”,
season_id=0,  # TODO: confirm season_id once available
start_date=date(2026, 4, 23),
end_date=date(2026, 5, 3),
),
ATPTournamentConfig(
tournament_name=“ATP Italian Open (Rome)”,
url=“https://www.oddspedia.com/us/a/tennis/italy/internazionali-bnl-d-italia”,
league_slug=“internazionali-bnl-d-italia”,
season_id=0,  # TODO: confirm season_id once available
start_date=date(2026, 5, 9),
end_date=date(2026, 5, 17),
),
ATPTournamentConfig(
tournament_name=“Roland Garros”,
url=“https://www.oddspedia.com/us/a/tennis/france/roland-garros”,
league_slug=“roland-garros”,
season_id=0,  # TODO: confirm season_id once available
start_date=date(2026, 5, 24),
end_date=date(2026, 6, 7),
),
ATPTournamentConfig(
tournament_name=“ATP Queen’s Club”,
url=“https://www.oddspedia.com/us/a/tennis/great-britain/queens-club-championships”,
league_slug=“queens-club-championships”,
season_id=0,  # TODO: confirm season_id once available
start_date=date(2026, 6, 15),
end_date=date(2026, 6, 21),
),
ATPTournamentConfig(
tournament_name=“Wimbledon”,
url=“https://www.oddspedia.com/us/a/tennis/great-britain/wimbledon”,
league_slug=“wimbledon”,
season_id=0,  # TODO: confirm season_id once available
start_date=date(2026, 6, 29),
end_date=date(2026, 7, 12),
),
ATPTournamentConfig(
tournament_name=“US Open”,
url=“https://www.oddspedia.com/us/a/tennis/usa/us-open”,
league_slug=“us-open”,
season_id=0,  # TODO: confirm season_id once available
start_date=date(2026, 8, 24),
end_date=date(2026, 9, 6),
),
]

def get_active_tournaments(*, today: Optional[date] = None) -> List[ATPTournamentConfig]:
“”“Return all tournaments currently in progress (or starting within 2 days).”””
if today is None:
today = datetime.now(timezone.utc).date()

```
active = [
    t for t in ATP_2026_TOURNAMENTS
    if t.start_date <= today <= t.end_date
    # Include tournaments starting within 2 days so we pick up pre-draw odds
    or (t.start_date - today).days <= 2 and (t.start_date - today).days >= 0
]
return active
```

def get_nearest_tournament(*, today: Optional[date] = None) -> ATPTournamentConfig:
“”“Return the tournament that is active or soonest upcoming.”””
if today is None:
today = datetime.now(timezone.utc).date()

```
active = get_active_tournaments(today=today)
if active:
    # If multiple active (overlapping weeks), return earliest-ending first
    return sorted(active, key=lambda t: t.end_date)[0]

# Nothing active — return the next upcoming one
upcoming = [t for t in ATP_2026_TOURNAMENTS if t.start_date > today]
if upcoming:
    return sorted(upcoming, key=lambda t: t.start_date)[0]

# Fallback: most recent past tournament
past = [t for t in ATP_2026_TOURNAMENTS if t.end_date < today]
return sorted(past, key=lambda t: t.end_date)[-1]
```

def get_atp_config() -> ATPTournamentConfig:
“””
Resolve the active ATP tournament config.

```
Priority:
1. Explicit env vars (ODDSPEDIA_ATP_URL + ODDSPEDIA_ATP_SLUG + ODDSPEDIA_ATP_SEASON_ID)
2. Auto-detected from calendar (nearest active/upcoming tournament)
"""
env_url = os.getenv("ODDSPEDIA_ATP_URL", "").strip()
env_slug = os.getenv("ODDSPEDIA_ATP_SLUG", "").strip()
env_season_id = os.getenv("ODDSPEDIA_ATP_SEASON_ID", "").strip()

if env_url and env_slug and env_season_id:
    try:
        season_id = int(env_season_id)
    except ValueError:
        raise ValueError(
            f"ODDSPEDIA_ATP_SEASON_ID must be an integer, got: {env_season_id!r}"
        )
    print(
        f"[atp_config] Using env override: url={env_url} "
        f"slug={env_slug} season_id={season_id}"
    )
    return ATPTournamentConfig(
        tournament_name="(env override)",
        url=env_url,
        league_slug=env_slug,
        season_id=season_id,
        start_date=date.today(),
        end_date=date.today(),
    )

cfg = get_nearest_tournament()
print(
    f"[atp_config] Auto-selected tournament: {cfg.tournament_name} "
    f"({cfg.start_date} – {cfg.end_date}) "
    f"slug={cfg.league_slug} season_id={cfg.season_id}"
)
return cfg
```

if **name** == “**main**”:
# Quick sanity check
cfg = get_atp_config()
print(f”Tournament : {cfg.tournament_name}”)
print(f”URL        : {cfg.url}”)
print(f”Slug       : {cfg.league_slug}”)
print(f”Season ID  : {cfg.season_id}”)
print(f”Dates      : {cfg.start_date} → {cfg.end_date}”)

```
active = get_active_tournaments()
print(f"\nActive tournaments ({len(active)}):")
for t in active:
    print(f"  - {t.tournament_name}")
```