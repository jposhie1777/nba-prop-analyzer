"""
k_discord_alerts.py — Send K prop pick alerts to Discord #strikeouts channel.

Reads today's FIRE and STRONG K picks from BigQuery and sends
rich embed messages split by OVER and UNDER.
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
K_TABLE = f"{PROJECT}.{DATASET}.k_picks_daily"

WEBHOOK_URL = os.getenv("DISCORD_K_WEBHOOK") or \
    "https://discord.com/api/webhooks/1493226751936036924/iwUUMtAA5GMmpdF1QBl45JnIYunAzF0UrfD_bdHxJSxf4KDb8Q8hfRuh-JsI2Ll0prJ2"

BOT_TOKEN = os.getenv("DISCORD_BOT_TOKEN", "")
K_CHANNEL_ID = os.getenv("DISCORD_K_CHANNEL_ID") or "1493224034752659517"
DISCORD_API = "https://discord.com/api/v10"

ET = ZoneInfo("America/New_York")
TODAY = datetime.now(ET).date()

GRADE_COLORS = {
    "FIRE":   0xEF4444,   # red
    "STRONG": 0xF59E0B,   # amber
    "LEAN":   0x6366F1,   # indigo
}

SIDE_EMOJI = {
    "OVER":  "\U0001f525",  # fire
    "UNDER": "\u2744\ufe0f",  # snowflake
}

GRADE_EMOJI = {
    "FIRE":   "\U0001f534",   # red circle
    "STRONG": "\U0001f7e0",   # orange circle
    "LEAN":   "\U0001f535",   # blue circle
}


def _fmt_odds(val):
    if val is None:
        return "\u2014"
    v = int(val)
    return f"+{v}" if v > 0 else str(v)


def fetch_top_k_picks():
    """Fetch today's FIRE, STRONG, and LEAN K picks — best line per pitcher."""
    client = bigquery.Client(project=PROJECT)
    query = f"""
    SELECT *
    FROM `{K_TABLE}`
    WHERE run_date = @run_date
      AND grade IN ('FIRE', 'STRONG', 'LEAN')
      AND (game_date IS NULL OR game_date > CURRENT_TIMESTAMP())
      AND (is_best_line = TRUE OR is_best_line IS NULL)
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY pitcher_id
        ORDER BY score DESC
    ) = 1
    ORDER BY score DESC
    """
    params = [bigquery.ScalarQueryParameter("run_date", "DATE", TODAY.isoformat())]
    job = client.query(query, job_config=bigquery.QueryJobConfig(query_parameters=params))
    return list(job.result())


def _build_pick_line(p):
    """Build a single pick line for an embed."""
    side = p.get("side", "OVER")
    side_em = SIDE_EMOJI.get(side, "")
    grade_em = GRADE_EMOJI.get(p.get("grade"), "")
    pitcher = p.get("pitcher_name", "?")
    team = p.get("team_code", "")
    opp = p.get("opp_team_code", "")
    line = p.get("line") or 0
    score = int(p.get("score") or 0)
    odds = _fmt_odds(p.get("best_price"))
    book = p.get("best_book", "")
    expected_k = p.get("expected_k") or 0

    k9 = p.get("k_per_9") or 0
    k_pct = p.get("k_pct") or 0
    whiff = p.get("arsenal_whiff_avg") or 0
    avg_l10 = p.get("avg_l10") or 0
    opp_rank = p.get("opp_team_k_rank")

    opp_str = f"#{opp_rank}" if opp_rank else "?"

    # Build FD direct bet link from market/selection IDs
    fd_mid = p.get("fd_market_id")
    fd_sid = p.get("fd_selection_id")
    if fd_mid and fd_sid:
        fd_link = f"https://sportsbook.fanduel.com/addToBetslip?marketId[0]={fd_mid}&selectionId[0]={fd_sid}"
        bet_part = f" \u2022 [Bet on FanDuel]({fd_link})"
    else:
        bet_part = ""

    # Build odds link (fallback to deep_link_desktop)
    desktop_link = p.get("deep_link_desktop")
    odds_part = f"[{odds} {book}]({desktop_link})" if desktop_link else f"{odds} {book}"

    # Show projected Ks
    proj_part = f"Projected **{expected_k:.1f} Ks**" if expected_k > 0 else ""

    return (
        f"{side_em} {grade_em} **{pitcher}** ({team} vs {opp}) \u2014 **{side} {line:.1f}**\n"
        f"> K-Pulse **{score}** \u2022 {odds_part}{bet_part}\n"
        f"> {proj_part + ' | ' if proj_part else ''}"
        f"K/9 **{k9:.1f}** \u2022 K% **{k_pct:.0f}%** \u2022 "
        f"Whiff **{whiff:.0f}%** \u2022 L10 avg **{avg_l10:.1f}** \u2022 Opp K-rank {opp_str}"
    )


def build_embeds(picks):
    """Build Discord embeds — separate sections for OVER and UNDER picks."""
    embeds = []

    # Split into overs and unders
    overs = [p for p in picks if p.get("side") == "OVER" and p.get("grade") in ("FIRE", "STRONG")]
    unders = [p for p in picks if p.get("side") == "UNDER" and p.get("grade") in ("FIRE", "STRONG")]
    leans = [p for p in picks if p.get("grade") == "LEAN"]

    # Header embed with today's summary
    fire_count = sum(1 for p in picks if p.get("grade") == "FIRE")
    strong_count = sum(1 for p in picks if p.get("grade") == "STRONG")
    lean_count = len(leans)

    embeds.append({
        "title": f"\u26a1 K Prop Picks \u2014 {TODAY.strftime('%b %d')}",
        "description": (
            f"\U0001f534 **{fire_count}** FIRE \u2022 "
            f"\U0001f7e0 **{strong_count}** STRONG \u2022 "
            f"\U0001f535 **{lean_count}** LEAN\n\n"
            f"*Powered by K-Pulse \u2014 self-learning strikeout model*"
        ),
        "color": 0x8B5CF6,
        "timestamp": datetime.now(ET).isoformat(),
    })

    # OVER picks
    if overs:
        lines = [_build_pick_line(p) for p in overs[:8]]
        desc = "\n\n".join(lines)
        if len(desc) > 4000:
            desc = desc[:3997] + "..."
        embeds.append({
            "title": f"\U0001f525 K OVERS \u2014 {TODAY.strftime('%b %d')}",
            "description": desc,
            "color": 0xEF4444,
            "footer": {"text": "High strikeout pitchers vs K-prone lineups"},
            "timestamp": datetime.now(ET).isoformat(),
        })

    # UNDER picks
    if unders:
        lines = [_build_pick_line(p) for p in unders[:8]]
        desc = "\n\n".join(lines)
        if len(desc) > 4000:
            desc = desc[:3997] + "..."
        embeds.append({
            "title": f"\u2744\ufe0f K UNDERS \u2014 {TODAY.strftime('%b %d')}",
            "description": desc,
            "color": 0x3B82F6,
            "footer": {"text": "Low K pitchers vs disciplined lineups"},
            "timestamp": datetime.now(ET).isoformat(),
        })

    # LEAN picks (lower section, compact)
    if leans:
        lean_overs = [p for p in leans if p.get("side") == "OVER"][:4]
        lean_unders = [p for p in leans if p.get("side") == "UNDER"][:4]
        lean_lines = []
        for p in lean_overs + lean_unders:
            side_em = SIDE_EMOJI.get(p.get("side"), "")
            pitcher = p.get("pitcher_name", "?")
            line = p.get("line") or 0
            score = int(p.get("score") or 0)
            odds = _fmt_odds(p.get("best_price"))
            side = p.get("side", "OVER")
            lean_lines.append(
                f"{side_em} **{pitcher}** {side} {line:.1f} \u2022 "
                f"K-Pulse {score} \u2022 {odds}"
            )
        if lean_lines:
            embeds.append({
                "title": f"\U0001f535 LEANS \u2014 {TODAY.strftime('%b %d')}",
                "description": "\n".join(lean_lines),
                "color": 0x6366F1,
                "footer": {"text": "Moderate edge \u2014 use with caution"},
                "timestamp": datetime.now(ET).isoformat(),
            })

    return embeds


def _purge_k_channel():
    """Delete all messages in the K channel before sending fresh picks."""
    import time as _time

    if not BOT_TOKEN or not K_CHANNEL_ID:
        log.info("Skipping K channel purge (need BOT_TOKEN + K_CHANNEL_ID)")
        return

    headers = {
        "Authorization": f"Bot {BOT_TOKEN}",
        "Content-Type": "application/json",
        "User-Agent": "PulseSports/1.0",
    }

    total_deleted = 0
    while True:
        fetch_url = f"{DISCORD_API}/channels/{K_CHANNEL_ID}/messages?limit=100"
        req = Request(fetch_url, headers=headers, method="GET")
        try:
            with urlopen(req, timeout=10) as resp:
                messages = json.loads(resp.read().decode("utf-8"))
        except Exception as exc:
            log.error("Failed to fetch K channel messages for purge: %s", exc)
            break

        if not messages:
            break

        msg_ids = [m["id"] for m in messages]

        if len(msg_ids) == 1:
            del_url = f"{DISCORD_API}/channels/{K_CHANNEL_ID}/messages/{msg_ids[0]}"
            req = Request(del_url, headers=headers, method="DELETE")
            try:
                with urlopen(req, timeout=10):
                    total_deleted += 1
            except Exception as exc:
                log.warning("Single delete failed: %s", exc)
                break
        else:
            bulk_url = f"{DISCORD_API}/channels/{K_CHANNEL_ID}/messages/bulk-delete"
            data = json.dumps({"messages": msg_ids}).encode("utf-8")
            req = Request(bulk_url, data=data, headers=headers, method="POST")
            try:
                with urlopen(req, timeout=10):
                    total_deleted += len(msg_ids)
            except Exception as exc:
                err_str = str(exc)
                if "429" in err_str:
                    _time.sleep(2)
                    continue
                log.warning("Bulk delete failed: %s — trying individual deletes", exc)
                for mid in msg_ids:
                    del_url = f"{DISCORD_API}/channels/{K_CHANNEL_ID}/messages/{mid}"
                    req = Request(del_url, headers=headers, method="DELETE")
                    try:
                        with urlopen(req, timeout=10):
                            total_deleted += 1
                    except Exception:
                        pass
                    _time.sleep(0.3)
                break

        _time.sleep(1)

    log.info("Purged %s messages from K channel", total_deleted)


def send_to_discord(embeds):
    """Send embeds to Discord webhook.

    Every batch posts with SUPPRESS_NOTIFICATIONS (flags=4096) — the
    consolidated summary in summary_alert.py is the only ping per run.
    """
    if not WEBHOOK_URL:
        log.warning("No Discord K webhook URL configured")
        return

    # Discord allows max 10 embeds per message — split if needed
    for i in range(0, len(embeds), 10):
        batch = embeds[i:i + 10]
        body = {"embeds": batch, "flags": 4096}  # SUPPRESS_NOTIFICATIONS
        payload = json.dumps(body).encode("utf-8")
        req = Request(
            WEBHOOK_URL,
            data=payload,
            headers={
                "Content-Type": "application/json",
                "User-Agent": "PulseSports/1.0",
            },
            method="POST",
        )
        try:
            with urlopen(req, timeout=10) as resp:
                log.info("Discord K webhook sent: HTTP %s", resp.status)
        except Exception as exc:
            log.error("Discord K webhook failed: %s", exc)


def main():
    log.info("Fetching K picks for %s", TODAY)
    picks = fetch_top_k_picks()

    fire = sum(1 for p in picks if p.get("grade") == "FIRE")
    strong = sum(1 for p in picks if p.get("grade") == "STRONG")
    lean = sum(1 for p in picks if p.get("grade") == "LEAN")
    log.info("Found %s FIRE + %s STRONG + %s LEAN K picks", fire, strong, lean)

    if not picks:
        log.info("No actionable K picks today \u2014 skipping Discord alert")
        return

    _purge_k_channel()
    embeds = build_embeds(picks)
    send_to_discord(embeds)
    log.info("Discord K alerts sent to #strikeouts")


if __name__ == "__main__":
    main()
