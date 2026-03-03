from __future__ import annotations

import html
import re
from datetime import datetime, timezone
from typing import Any, Dict, List, Tuple

from atp_models import (
    HeadToHeadMatchRow,
    MatchResultRow,
    MatchScheduleRow,
    TopSeedRow,
    TournamentMonthRow,
    TournamentOverviewRow,
    TournamentRow,
)


def utc_now_iso() -> str:
    return datetime.now(tz=timezone.utc).replace(microsecond=0).isoformat()


def _strip_tags(value: str) -> str:
    return re.sub(r"\s+", " ", re.sub(r"<[^>]+>", " ", html.unescape(value))).strip()


def _find(pattern: str, text: str) -> str | None:
    m = re.search(pattern, text, flags=re.IGNORECASE | re.DOTALL)
    return _strip_tags(m.group(1)) if m else None


def _find_href(pattern: str, text: str) -> str | None:
    m = re.search(pattern, text, flags=re.IGNORECASE | re.DOTALL)
    return m.group(1).strip() if m else None


def normalize_calendar(calendar_json: Dict[str, Any], snapshot_ts_utc: str | None = None) -> Tuple[List[TournamentMonthRow], List[TournamentRow]]:
    ts = snapshot_ts_utc or utc_now_iso()
    month_rows: List[TournamentMonthRow] = []
    tournament_rows: List[TournamentRow] = []

    for month_block in calendar_json.get("TournamentDates", []) or []:
        display_date = month_block.get("DisplayDate")
        month_rows.append(TournamentMonthRow(ts, display_date, month_block.get("IsExpanded"), month_block.get("NoEvents")))

        for tournament in month_block.get("Tournaments", []) or []:
            tournament_rows.append(
                TournamentRow(
                    ts,
                    display_date,
                    str(tournament.get("Id")),
                    tournament.get("Name"),
                    tournament.get("Location"),
                    tournament.get("FormattedDate"),
                    tournament.get("Type"),
                    tournament.get("EventType"),
                    tournament.get("EventTypeDetail"),
                    tournament.get("Surface"),
                    tournament.get("IndoorOutdoor"),
                    tournament.get("SglDrawSize"),
                    tournament.get("DblDrawSize"),
                    tournament.get("TotalFinancialCommitment"),
                    tournament.get("PrizeMoneyDetails"),
                    tournament.get("ScoresUrl"),
                    tournament.get("DrawsUrl"),
                    tournament.get("ScheduleUrl"),
                    tournament.get("TournamentSiteUrl"),
                    tournament.get("TournamentOverviewUrl"),
                    tournament.get("TicketsUrl"),
                    tournament.get("TicketHotline"),
                    tournament.get("PhoneNumber"),
                    tournament.get("Email"),
                    tournament.get("SchedulePrintUrl"),
                    tournament.get("SinglesDrawPrintUrl"),
                    tournament.get("DoublesDrawPrintUrl"),
                    tournament.get("QualySinglesDrawPrintUrl"),
                    tournament.get("IsLive"),
                    tournament.get("IsPastEvent"),
                )
            )

    return month_rows, tournament_rows


def normalize_overview(tournament_id: str, overview_json: Dict[str, Any], snapshot_ts_utc: str | None = None, include_raw_json: bool = False) -> TournamentOverviewRow:
    ts = snapshot_ts_utc or utc_now_iso()
    return TournamentOverviewRow(
        ts,
        str(tournament_id),
        overview_json.get("SponsorTitle"),
        overview_json.get("Bio"),
        overview_json.get("SinglesDrawSize"),
        overview_json.get("DoublesDrawSize"),
        overview_json.get("Surface"),
        overview_json.get("SurfaceSubCat"),
        overview_json.get("InOutdoor"),
        overview_json.get("Prize"),
        overview_json.get("TotalFinancialCommitment"),
        overview_json.get("Location"),
        overview_json.get("WebsiteUrl"),
        overview_json.get("EventType"),
        overview_json.get("EventTypeDetail"),
        overview_json.get("FbLink"),
        overview_json.get("TwLink"),
        overview_json.get("IgLink"),
        overview_json if include_raw_json else None,
    )


def normalize_top_seeds(tournament_id: str, event_year: int, top_seeds_json: Dict[str, Any], snapshot_ts_utc: str | None = None) -> List[TopSeedRow]:
    ts = snapshot_ts_utc or utc_now_iso()
    return [
        TopSeedRow(
            ts,
            str(tournament_id),
            int(event_year),
            player.get("SeedNumber"),
            player.get("FullName"),
            player.get("WinLoss"),
            player.get("BestFinish"),
            player.get("PlayerProfileUrl"),
            player.get("PlayerCountryUrl"),
        )
        for player in (top_seeds_json.get("SinglePlayers", []) or [])
    ]


def normalize_head_to_head(left_player_id: str, right_player_id: str, h2h_json: Dict[str, Any], snapshot_ts_utc: str | None = None) -> List[HeadToHeadMatchRow]:
    ts = snapshot_ts_utc or utc_now_iso()
    rows: List[HeadToHeadMatchRow] = []
    for tournament in h2h_json.get("Tournaments", []) or []:
        for match in tournament.get("MatchResults", []) or []:
            round_info = match.get("Round") or {}
            rows.append(
                HeadToHeadMatchRow(
                    ts,
                    left_player_id,
                    right_player_id,
                    tournament.get("EventId"),
                    tournament.get("EventYear"),
                    tournament.get("TournamentName"),
                    tournament.get("Surface"),
                    tournament.get("InOutdoorDisplay"),
                    match.get("MatchId"),
                    match.get("Winner"),
                    match.get("IsDoubles"),
                    match.get("IsQualifier"),
                    round_info.get("ShortName"),
                    round_info.get("LongName"),
                    match.get("MatchTime"),
                    match.get("IsMatchLive"),
                )
            )
    return rows


def normalize_match_schedule_html(tournament_slug: str, tournament_id: str, schedule_html: str, snapshot_ts_utc: str | None = None) -> List[MatchScheduleRow]:
    ts = snapshot_ts_utc or utc_now_iso()
    day = _find(r'<h4 class="day">\s*(.*?)\s*</h4>', schedule_html)
    rows: List[MatchScheduleRow] = []

    chunks = schedule_html.split('<div class="schedule"')
    for chunk in chunks[1:]:
        row_html = '<div class="schedule"' + chunk
        court_name = _find(r"<strong>([^<]+)</strong>", row_html)
        start_label = _find(r'<span class="matchtime">(.*?)</span>', row_html)
        schedule_type = _find(r'<div class="schedule-type">\s*(.*?)\s*</div>', row_html)
        status_text = _find(r'<div class="status">\s*(.*?)\s*</div>', row_html)

        players = re.findall(r'<div class="name">(.*?)</div>', row_html, flags=re.IGNORECASE | re.DOTALL)
        hrefs = re.findall(r'<div class="name">\s*<a href="([^"]+)"', row_html, flags=re.IGNORECASE | re.DOTALL)
        seeds = re.findall(r'<div class="rank">\s*<span>\((.*?)\)</span>', row_html, flags=re.IGNORECASE | re.DOTALL)

        p1_name = _strip_tags(players[0]) if len(players) > 0 else None
        p2_name = _strip_tags(players[1]) if len(players) > 1 else None
        p1_url = hrefs[0] if len(hrefs) > 0 else None
        p2_url = hrefs[1] if len(hrefs) > 1 else None
        p1_seed = seeds[0] if len(seeds) > 0 else None
        p2_seed = seeds[1] if len(seeds) > 1 else None

        rows.append(
            MatchScheduleRow(
                ts,
                tournament_slug,
                tournament_id,
                day,
                court_name,
                schedule_type,
                start_label,
                p1_name,
                p1_url,
                p1_seed,
                p2_name,
                p2_url,
                p2_seed,
                status_text,
            )
        )

    return rows


def normalize_match_results_html(tournament_slug: str, tournament_id: str, results_html: str, snapshot_ts_utc: str | None = None) -> List[MatchResultRow]:
    ts = snapshot_ts_utc or utc_now_iso()
    day_label = _find(r"<div class=\"tournament-day\">\s*<h4>\s*(.*?)\s*</h4>", results_html)
    rows: List[MatchResultRow] = []

    chunks = results_html.split('<div class="match">')
    for chunk in chunks[1:]:
        row_html = chunk
        round_and_court = _find(r"<span><strong>(.*?)</strong></span>", row_html)
        duration = _find(r"<div class=\"match-header\">.*?<span><strong>.*?</strong></span>\s*<span>(.*?)</span>", row_html)
        umpire = _find(r"<div class=\"match-umpire\">\s*(.*?)\s*</div>", row_html)
        notes = _find(r"<div class=\"match-notes\">\s*(.*?)\s*</div>", row_html)

        stats_items = re.findall(r'<div class="stats-item">(.*?)</div>\s*</div>', row_html, flags=re.IGNORECASE | re.DOTALL)
        p1_html = stats_items[0] if len(stats_items) > 0 else ""
        p2_html = stats_items[1] if len(stats_items) > 1 else ""

        p1_name = _find(r'<div class="name">\s*<a [^>]+>(.*?)</a>', p1_html)
        p2_name = _find(r'<div class="name">\s*<a [^>]+>(.*?)</a>', p2_html)
        p1_url = _find_href(r'<div class="name">\s*<a href="([^"]+)"', p1_html)
        p2_url = _find_href(r'<div class="name">\s*<a href="([^"]+)"', p2_html)
        p1_is_winner = "class=\"winner\"" in p1_html
        p2_is_winner = "class=\"winner\"" in p2_html
        p1_scores = " ".join(re.findall(r"<div class=\"score-item\">\s*<span>(\d+)</span>", p1_html)) or None
        p2_scores = " ".join(re.findall(r"<div class=\"score-item\">\s*<span>(\d+)</span>", p2_html)) or None

        h2h_url = _find_href(r'<a href="([^"]+)">H2H</a>', row_html)
        stats_url = _find_href(r'<a href="([^"]+)">Stats</a>', row_html)

        rows.append(
            MatchResultRow(
                ts,
                tournament_slug,
                tournament_id,
                day_label,
                round_and_court,
                duration,
                p1_name,
                p1_url,
                p1_is_winner,
                p1_scores,
                p2_name,
                p2_url,
                p2_is_winner,
                p2_scores,
                h2h_url,
                stats_url,
                umpire,
                notes,
            )
        )

    return rows
