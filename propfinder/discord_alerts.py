"""
discord_alerts.py — Send HR pick alerts to Discord via webhook.

Reads today's IDEAL and FAVORABLE picks from BigQuery and sends
rich embed messages to the configured Discord channel.
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

WEBHOOK_URL = os.getenv(
    "DISCORD_HR_WEBHOOK",
    "https://discord.com/api/webhooks/1493058679153623060/hEI1Eqxbdc2MyDFG-dAaAgoqMk46J11M9EKzM3ZJkSj-cZJZWvwSE1RM5u0MxlUMmZnD",
)

ET = ZoneInfo("America/New_York")
TODAY = datetime.now(ET).date()

GRADE_COLORS = {
    "IDEAL": 0x22C55E,       # green
    "FAVORABLE": 0xF59E0B,   # amber
}

GRADE_EMOJI = {
    "IDEAL": "\U0001f7e2",       # green circle
    "FAVORABLE": "\U0001f7e1",   # yellow circle
}


def _hand_label(side):
    s = (side or "R").upper()
    return "LHB" if s == "L" else "SHB" if s == "S" else "RHB"


def _pitcher_hand(side):
    s = (side or "R").upper()
    return "LHP" if s.startswith("L") else "RHP"


def _fmt_odds(val):
    if val is None:
        return "—"
    v = int(val)
    return f"+{v}" if v > 0 else str(v)


def _build_dk_link(outcome_code, event_id):
    if not outcome_code or not event_id:
        return None
    return f"https://sportsbook.draftkings.com/event/{event_id}?outcomes={outcome_code}"


def _build_fd_link(market_id, selection_id):
    if not market_id or not selection_id:
        return None
    return f"https://sportsbook.fanduel.com/addToBetslip?marketId[0]={market_id}&selectionId[0]={selection_id}"


def _best_book_line(pick):
    """Return (odds_str, book_label, deeplink) for DK or FD only."""
    price = pick.get("hr_odds_best_price")
    book = (pick.get("hr_odds_best_book") or "").strip()

    dk_link = _build_dk_link(pick.get("dk_outcome_code"), pick.get("dk_event_id"))
    fd_link = _build_fd_link(pick.get("fd_market_id"), pick.get("fd_selection_id"))

    # If best book IS DK or FD, use that price + link
    if "draftkings" in book.lower() or "dk" in book.lower():
        return _fmt_odds(price), "DK", dk_link
    if "fanduel" in book.lower() or "fd" in book.lower():
        return _fmt_odds(price), "FD", fd_link

    # Best book is neither — show best price, link to DK (preferred) or FD
    odds_str = _fmt_odds(price)
    if dk_link:
        return odds_str, "DK", dk_link
    if fd_link:
        return odds_str, "FD", fd_link
    return odds_str, "", None


def fetch_top_picks():
    """Fetch today's IDEAL and FAVORABLE picks from BigQuery."""
    client = bigquery.Client(project=PROJECT)
    query = f"""
    SELECT
      batter_name, bat_side, pitcher_name, pitcher_hand,
      score, grade, why, flags,
      iso, slg, l15_ev, l15_barrel_pct, season_ev, season_barrel_pct,
      l15_hard_hit_pct, hr_fb_pct,
      p_hr9_vs_hand, p_hr_fb_pct, p_barrel_pct, p_fb_pct,
      hr_odds_best_price, hr_odds_best_book,
      dk_outcome_code, dk_event_id, fd_market_id, fd_selection_id,
      weather_indicator, game_temp, wind_speed, ballpark_name,
      home_moneyline, away_moneyline, over_under,
      game_pk
    FROM `{HR_TABLE}`
    WHERE run_date = @run_date
      AND grade IN ('IDEAL', 'FAVORABLE')
    QUALIFY ROW_NUMBER() OVER (
      PARTITION BY batter_name, pitcher_name
      ORDER BY score DESC
    ) = 1
    ORDER BY score DESC
    """
    params = [bigquery.ScalarQueryParameter("run_date", "DATE", TODAY.isoformat())]
    job = client.query(query, job_config=bigquery.QueryJobConfig(query_parameters=params))
    return list(job.result())


def build_embeds(picks):
    """Build Discord embeds from picks, grouped by grade."""
    embeds = []

    # Weak spots — IDEAL/FAVORABLE picks where pitcher is exploitable
    weak_spots = [
        p for p in picks
        if (p.get("p_hr9_vs_hand") or 0) >= 1.8
        and (p.get("p_hr_fb_pct") or 0) >= 15
        and p.get("grade") in ("IDEAL", "FAVORABLE")
    ]
    if weak_spots:
        weak_spots = weak_spots[:6]
        ws_lines = []
        for p in weak_spots:
            batter = p.get("batter_name", "?")
            bat = _hand_label(p.get("bat_side"))
            pitcher = p.get("pitcher_name", "?")
            phand = _pitcher_hand(p.get("pitcher_hand"))
            score = int(p.get("score") or 0)
            odds, book_label, link = _best_book_line(p)
            hr9 = p.get("p_hr9_vs_hand") or 0
            hrfb = p.get("p_hr_fb_pct") or 0
            barrel = p.get("p_barrel_pct") or 0
            grade_em = GRADE_EMOJI.get(p.get("grade"), "")

            odds_part = f"[{odds} {book_label}]({link})" if link else odds
            ws_lines.append(
                f"{grade_em} **{batter}** ({bat}) vs {pitcher} ({phand})\n"
                f"> Pulse **{score}** \u2022 {odds_part}\n"
                f"> P-HR/9 **{hr9:.2f}** \u2022 HR/FB **{hrfb:.1f}%** \u2022 P-Barrel {barrel:.1f}%"
            )

        embeds.append({
            "title": f"\U0001f3af Pitcher Weak Spots \u2014 {TODAY.strftime('%b %d')}",
            "description": "\n\n".join(ws_lines),
            "color": 0xEF4444,  # red
            "footer": {"text": "Pitchers with HR/9 \u2265 1.80 + HR/FB% \u2265 15%"},
            "timestamp": datetime.now(ET).isoformat(),
        })

    for grade in ("IDEAL", "FAVORABLE"):
        group = [p for p in picks if p.get("grade") == grade]
        if not group:
            continue
        group = group[:8]  # Top 8 per grade to stay within Discord limits

        emoji = GRADE_EMOJI.get(grade, "")
        color = GRADE_COLORS.get(grade, 0x6366F1)

        lines = []
        for p in group:
            batter = p.get("batter_name", "?")
            bat = _hand_label(p.get("bat_side"))
            pitcher = p.get("pitcher_name", "?")
            phand = _pitcher_hand(p.get("pitcher_hand"))
            score = int(p.get("score") or 0)
            odds, book_label, link = _best_book_line(p)

            iso = p.get("iso")
            l15_ev = p.get("l15_ev")
            l15_bar = p.get("l15_barrel_pct")
            p_hr9 = p.get("p_hr9_vs_hand")

            stat_parts = []
            if iso is not None:
                stat_parts.append(f"ISO {iso:.3f}")
            if l15_ev is not None:
                stat_parts.append(f"EV {l15_ev:.1f}")
            if l15_bar is not None:
                stat_parts.append(f"Barrel {l15_bar:.0f}%")
            if p_hr9 is not None:
                stat_parts.append(f"P-HR/9 {p_hr9:.2f}")
            stats_str = " | ".join(stat_parts)

            weather = p.get("weather_indicator") or ""
            temp = p.get("game_temp")
            park = p.get("ballpark_name") or ""
            weather_str = ""
            if weather or temp:
                weather_parts = []
                if weather:
                    w_emoji = "\U0001f7e2" if weather == "Green" else "\U0001f7e1" if weather == "Yellow" else "\U0001f534"
                    weather_parts.append(w_emoji)
                if temp:
                    weather_parts.append(f"{int(temp)}\u00b0")
                if park:
                    weather_parts.append(park)
                weather_str = f" ({' '.join(weather_parts)})"

            odds_part = f"[{odds} {book_label}]({link})" if link else odds
            lines.append(
                f"**{batter}** ({bat}) vs {pitcher} ({phand})\n"
                f"> Pulse **{score}** \u2022 {odds_part}{weather_str}\n"
                f"> {stats_str}"
            )

        description = "\n\n".join(lines)
        if len(description) > 4000:
            description = description[:3997] + "..."

        embeds.append({
            "title": f"{emoji} {grade} HR Matchups — {TODAY.strftime('%b %d')}",
            "description": description,
            "color": color,
            "footer": {"text": "Pulse Sports Analytics"},
            "timestamp": datetime.now(ET).isoformat(),
        })

    return embeds


def send_to_discord(embeds):
    """Send embeds to Discord webhook."""
    if not WEBHOOK_URL:
        log.warning("No Discord webhook URL configured")
        return

    payload = json.dumps({"embeds": embeds}).encode("utf-8")
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
            log.info("Discord webhook sent: HTTP %s", resp.status)
    except Exception as exc:
        log.error("Discord webhook failed: %s", exc)


def main():
    log.info("Fetching HR picks for %s", TODAY)
    picks = fetch_top_picks()

    ideal = sum(1 for p in picks if p.get("grade") == "IDEAL")
    fav = sum(1 for p in picks if p.get("grade") == "FAVORABLE")
    log.info("Found %s IDEAL + %s FAVORABLE picks", ideal, fav)

    if not picks:
        log.info("No IDEAL/FAVORABLE picks today — skipping Discord alert")
        return

    embeds = build_embeds(picks)
    send_to_discord(embeds)
    log.info("Discord HR alerts sent")


if __name__ == "__main__":
    main()
