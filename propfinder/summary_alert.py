"""
summary_alert.py — Send a single consolidated PropFinder summary to Discord.

Counts today's HR, hit, and K picks from BigQuery and posts ONE non-silent
message to the unified summary channel. The per-channel alerts (HR / hit / K)
all run silent now, so this is the only push notification users get per run.
"""

import json
import logging
import os
from datetime import datetime
from urllib.request import Request, urlopen
from zoneinfo import ZoneInfo

from google.cloud import bigquery

log = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

PROJECT = os.getenv("GOOGLE_CLOUD_PROJECT", "graphite-flare-477419-h7")
DATASET = "propfinder"
HR_TABLE = f"{PROJECT}.{DATASET}.hr_picks_daily"
HIT_TABLE = f"{PROJECT}.{DATASET}.hit_picks_daily"
K_TABLE = f"{PROJECT}.{DATASET}.k_picks_daily"

BOT_TOKEN = os.getenv("DISCORD_BOT_TOKEN", "")
DISCORD_API = "https://discord.com/api/v10"

SUMMARY_CHANNEL_ID = os.getenv("DISCORD_SUMMARY_CHANNEL_ID") or "1498430553228312718"
HR_CHANNEL_ID = os.getenv("DISCORD_HR_CHANNEL_ID") or "1493058403990507622"
HIT_CHANNEL_ID = os.getenv("DISCORD_HIT_CHANNEL_ID") or "1493361988338974882"
K_CHANNEL_ID = os.getenv("DISCORD_K_CHANNEL_ID") or "1493224034752659517"

ET = ZoneInfo("America/New_York")
TODAY = datetime.now(ET).date()


def _count_by_grade(table, grades):
    """Return {grade: count} for today's picks. Empty dict on error."""
    bq = bigquery.Client(project=PROJECT)
    sql = f"""
        SELECT grade, COUNT(*) AS n
        FROM `{table}`
        WHERE run_date = @run_date
          AND grade IN UNNEST(@grades)
        GROUP BY grade
    """
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("run_date", "DATE", TODAY.isoformat()),
            bigquery.ArrayQueryParameter("grades", "STRING", list(grades)),
        ]
    )
    try:
        rows = bq.query(sql, job_config=job_config).result()
        return {row["grade"]: row["n"] for row in rows}
    except Exception as exc:
        log.warning("Count query failed for %s: %s", table, exc)
        return {}


def _fmt_segment(label, channel_id, counts, grade_order):
    """Format a single sport segment, e.g. '⚾ HR: 4 IDEAL · 12 FAVORABLE → <#…>'."""
    parts = []
    for g in grade_order:
        n = counts.get(g, 0)
        if n:
            parts.append(f"{n} {g}")
    if not parts:
        return f"{label}: no picks today"
    return f"{label}: {' · '.join(parts)} → <#{channel_id}>"


def build_summary():
    hr_counts = _count_by_grade(HR_TABLE, ("IDEAL", "FAVORABLE"))
    hit_counts = _count_by_grade(HIT_TABLE, ("FIRE", "STRONG", "LEAN"))
    k_counts = _count_by_grade(K_TABLE, ("FIRE", "STRONG", "LEAN"))

    total = sum(hr_counts.values()) + sum(hit_counts.values()) + sum(k_counts.values())

    lines = [
        f"\U0001f4e3 **PropFinder picks live — {TODAY.strftime('%b %d, %Y')}**",
        _fmt_segment("⚾ HR", HR_CHANNEL_ID, hr_counts, ("IDEAL", "FAVORABLE")),
        _fmt_segment("⚾ Hits", HIT_CHANNEL_ID, hit_counts, ("FIRE", "STRONG", "LEAN")),
        _fmt_segment("⚾ Ks", K_CHANNEL_ID, k_counts, ("FIRE", "STRONG", "LEAN")),
    ]
    return "\n".join(lines), total


def _post(content):
    if not BOT_TOKEN:
        log.warning("No DISCORD_BOT_TOKEN; skipping summary post")
        return
    if not SUMMARY_CHANNEL_ID:
        log.warning("No DISCORD_SUMMARY_CHANNEL_ID; skipping summary post")
        return

    url = f"{DISCORD_API}/channels/{SUMMARY_CHANNEL_ID}/messages"
    headers = {
        "Authorization": f"Bot {BOT_TOKEN}",
        "Content-Type": "application/json",
        "User-Agent": "PulseSports/1.0",
    }
    data = json.dumps({"content": content}).encode("utf-8")
    req = Request(url, data=data, headers=headers, method="POST")
    try:
        with urlopen(req, timeout=10) as resp:
            log.info("Summary posted: HTTP %s", resp.status)
    except Exception as exc:
        log.error("Summary post failed: %s", exc)


def main():
    content, total = build_summary()
    log.info("PropFinder summary (%s total picks):\n%s", total, content)
    _post(content)


if __name__ == "__main__":
    main()
