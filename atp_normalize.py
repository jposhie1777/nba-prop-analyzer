from __future__ import annotations

import html
import re
from datetime import date, datetime, timezone
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

    def _fmt_set_score(set_score: Any, tb_score: Any) -> str:
        if set_score is None:
            return ""
        if tb_score is None:
            return str(set_score)
        return f"{set_score}({tb_score})"

    def _extract_team_scores(team: Dict[str, Any]) -> Tuple[str | None, List[str]]:
        sets = team.get("Sets") or []
        display_parts: List[str] = []
        raw_parts: List[str] = []
        for item in sets:
            set_number = item.get("SetNumber")
            if not isinstance(set_number, int) or set_number <= 0:
                continue
            set_score = item.get("SetScore")
            tb_score = item.get("TieBreakScore")
            if set_score is None:
                continue
            raw_parts.append(str(set_score))
            display_parts.append(_fmt_set_score(set_score, tb_score))
        return (" ".join(raw_parts) if raw_parts else None), display_parts

    for tournament in h2h_json.get("Tournaments", []) or []:
        for match in tournament.get("MatchResults", []) or []:
            round_info = match.get("Round") or {}
            player_set_scores, player_display = _extract_team_scores(match.get("PlayerTeam") or {})
            opponent_set_scores, opponent_display = _extract_team_scores(match.get("OpponentTeam") or {})
            scoreline_display = None
            if player_display or opponent_display:
                pairs = []
                max_len = max(len(player_display), len(opponent_display))
                for idx in range(max_len):
                    p_val = player_display[idx] if idx < len(player_display) else ""
                    o_val = opponent_display[idx] if idx < len(opponent_display) else ""
                    if p_val or o_val:
                        pairs.append(f"{p_val}-{o_val}".strip("-"))
                scoreline_display = " ".join(pairs) if pairs else None

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
                    player_set_scores,
                    opponent_set_scores,
                    scoreline_display,
                )
            )
    return rows


def normalize_match_schedule_html(tournament_slug: str, tournament_id: str, schedule_html: str, snapshot_ts_utc: str | None = None, include_past: bool = False) -> List[MatchScheduleRow]:
    ts = snapshot_ts_utc or utc_now_iso()
    snapshot_date = datetime.fromisoformat(ts.replace("Z", "+00:00")).date()
    day = _find(r'<h4 class="day">\s*(.*?)\s*</h4>', schedule_html) or _find(
        r'<div class="tournament-day">\s*<h4>\s*(.*?)\s*</h4>', schedule_html
    )
    rows: List[MatchScheduleRow] = []

    def _parse_day_date(day_label: str | None) -> date | None:
        if not day_label:
            return None
        cleaned = re.sub(r"\s*\(?Day\s*\(?\d+\)?\)?\s*$", "", day_label, flags=re.IGNORECASE).strip()
        for fmt in ["%a, %d %B, %Y", "%A, %B %d, %Y", "%A, %d %B, %Y"]:
            try:
                return datetime.strptime(cleaned, fmt).date()
            except ValueError:
                continue
        return None

    day_date = _parse_day_date(day)

    def _extract_name_url_seed(block: str) -> Tuple[str | None, str | None, str | None]:
        name_text = _find(r'<div class="name">\s*(.*?)\s*</div>', block)
        profile_url = _find_href(r'<a href="([^"]+)"', block)
        seed = _find(r'<div class="rank">\s*<span>\((.*?)\)</span>', block)
        if name_text:
            name_text = name_text.replace("\xa0", " ").strip()
        return name_text, profile_url, seed

    chunks = schedule_html.split('<div class="schedule"')
    for chunk in chunks[1:]:
        row_html = '<div class="schedule"' + chunk
        court_name = _find(r'<div class="schedule-location-timestamp">\s*<span>\s*<strong>(.*?)</strong>', row_html)
        if not court_name:
            court_name = _find(r"<strong>([^<]+)</strong>", row_html)

        start_label = _find(r'<span class="matchtime">(.*?)</span>', row_html) or _find(
            r'data-displaytime="([^"]+)"', row_html
        )
        schedule_type = _find(r'<div class="schedule-type">\s*(.*?)\s*</div>', row_html)
        status_text = _find(r'<div class="status">\s*(.*?)\s*</div>', row_html)

        player_block = _find(r'<div class="player">(.*?)</div>\s*</div>\s*<div class="status">', row_html) or ""
        opponent_block = _find(r'<div class="opponent">(.*?)</div>\s*</div>\s*</div>\s*</div>', row_html) or ""

        p1_name, p1_url, p1_seed = _extract_name_url_seed(player_block)
        p2_name, p2_url, p2_seed = _extract_name_url_seed(opponent_block)

        if not p1_name or not p2_name or not p1_url or not p2_url:
            # Robust fallback: find all ATP player profile links directly in the schedule block.
            # This handles cases where <div class="rank"> precedes <a href> inside <div class="name">
            # (common for ATP opponents) which breaks the simpler block-extraction approach.
            player_links = re.findall(
                r'href="(/en/players/(?!atp-head-2-head)[^"]+/overview)"[^>]*>(.*?)</a>',
                row_html,
                flags=re.IGNORECASE | re.DOTALL,
            )
            if not p1_url and len(player_links) > 0:
                p1_url = player_links[0][0]
            if not p2_url and len(player_links) > 1:
                p2_url = player_links[1][0]
            if not p1_name and len(player_links) > 0:
                p1_name = _strip_tags(player_links[0][1]).replace("\xa0", " ").strip() or p1_name
            if not p2_name and len(player_links) > 1:
                p2_name = _strip_tags(player_links[1][1]).replace("\xa0", " ").strip() or p2_name
            # For WTA players (no profile link), fall back to name-div text
            if not p1_name or not p2_name:
                name_blocks = re.findall(r'<div class="name">\s*(.*?)\s*</div>', row_html, flags=re.IGNORECASE | re.DOTALL)
                if not p1_name and len(name_blocks) > 0:
                    p1_name = _strip_tags(name_blocks[0])
                if not p2_name and len(name_blocks) > 1:
                    p2_name = _strip_tags(name_blocks[1])

        status_lower = (status_text or "").strip().lower()
        court_lower = (court_name or "").strip().lower()
        p1_lower = (p1_name or "").lower()
        p2_lower = (p2_name or "").lower()

        # Keep this dataset focused on truly upcoming matches.
        if not include_past and day_date and day_date < snapshot_date:
            continue
        if "newsletter" in court_lower or "sign up" in court_lower:
            continue
        if "{{" in p1_lower or "{{" in p2_lower:
            continue
        if status_lower.startswith("defeat"):
            continue
        if not start_label and status_lower not in {"vs", "v"}:
            continue

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
    day_label = _find(r"<div class=['\"]tournament-day['\"]>\s*<h4>\s*(.*?)\s*</h4>", results_html)
    if not day_label:
        day_label = _find(r"<div class=['\"]tournament-day['\"]>\s*<h4>\s*([^<].*?)\s*<", results_html)

    def _parse_match_date(label: str | None) -> str | None:
        if not label:
            return None
        cleaned = re.sub(r"\s*\(?Day\s*\(?\d+\)?\)?\s*$", "", label, flags=re.IGNORECASE).strip()
        for fmt in ["%a, %d %B, %Y", "%A, %B %d, %Y", "%A, %d %B, %Y"]:
            try:
                return datetime.strptime(cleaned, fmt).date().isoformat()
            except ValueError:
                continue
        return None

    match_date = _parse_match_date(day_label)
    rows: List[MatchResultRow] = []

    def _extract_stats_items(match_html: str) -> List[str]:
        return [
            m.group(1)
            for m in re.finditer(
                r'<div class="stats-item">(.*?)(?=<div class="stats-item">|<div class="match-footer">)',
                match_html,
                flags=re.IGNORECASE | re.DOTALL,
            )
        ]

    def _extract_player(item_html: str) -> Tuple[str | None, str | None, bool, str | None]:
        name = _find(r'<div class="name">\s*(.*?)\s*</div>', item_html)
        profile_url = _find_href(r'<div class="name">\s*<a href="([^"]+)"', item_html)
        is_winner = "class=\"winner\"" in item_html

        score_text = None
        score_items = re.findall(r'<div class="score-item">(.*?)</div>', item_html, flags=re.IGNORECASE | re.DOTALL)
        if score_items:
            parts: List[str] = []
            for score_item in score_items:
                vals = re.findall(r"<span>(\d+)</span>", score_item)
                if not vals:
                    continue
                if len(vals) >= 2:
                    parts.append(f"{vals[0]}({vals[1]})")
                else:
                    parts.append(vals[0])
            score_text = " ".join(parts) if parts else None

        return name, profile_url, is_winner, score_text

    chunks = results_html.split('<div class="match">')
    for chunk in chunks[1:]:
        row_html = chunk
        round_and_court = _find(r"<span><strong>(.*?)</strong></span>", row_html)
        duration = _find(r"<div class=\"match-header\">.*?<span><strong>.*?</strong></span>\s*<span>(.*?)</span>", row_html)
        umpire = _find(r"<div class=\"match-umpire\">\s*(.*?)\s*</div>", row_html)
        notes = _find(r"<div class=\"match-notes\">\s*(.*?)\s*</div>", row_html)

        stats_items = _extract_stats_items(row_html)
        p1_html = stats_items[0] if len(stats_items) > 0 else ""
        p2_html = stats_items[1] if len(stats_items) > 1 else ""
        p1_name, p1_url, p1_is_winner, p1_scores = _extract_player(p1_html)
        p2_name, p2_url, p2_is_winner, p2_scores = _extract_player(p2_html)

        if not p1_name or not p2_name:
            # fallback for alternate layouts
            names = re.findall(r'<div class="name">\s*(.*?)\s*</div>', row_html, flags=re.IGNORECASE | re.DOTALL)
            hrefs = re.findall(r'<div class="name">\s*<a href="([^"]+)"', row_html, flags=re.IGNORECASE | re.DOTALL)
            if not p1_name and len(names) > 0:
                p1_name = _strip_tags(names[0])
            if not p2_name and len(names) > 1:
                p2_name = _strip_tags(names[1])
            if not p1_url and len(hrefs) > 0:
                p1_url = hrefs[0]
            if not p2_url and len(hrefs) > 1:
                p2_url = hrefs[1]

        h2h_url = _find_href(r'<a href="([^"]+)">H2H</a>', row_html)
        stats_url = _find_href(r'<a href="([^"]+)">Stats</a>', row_html)

        rows.append(
            MatchResultRow(
                ts,
                tournament_slug,
                tournament_id,
                day_label,
                match_date,
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
