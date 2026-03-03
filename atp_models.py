from __future__ import annotations

from dataclasses import asdict, dataclass
from typing import Any, Dict, Optional


@dataclass(frozen=True)
class TournamentMonthRow:
    snapshot_ts_utc: str
    display_month: str
    is_expanded: Optional[bool]
    no_events: Optional[int]

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class TournamentRow:
    snapshot_ts_utc: str
    month_display_date: str
    tournament_id: str
    name: Optional[str]
    location: Optional[str]
    formatted_date: Optional[str]
    type: Optional[str]
    event_type: Optional[str]
    event_type_detail: Optional[int]
    surface: Optional[str]
    indoor_outdoor: Optional[str]
    sgl_draw_size: Optional[int]
    dbl_draw_size: Optional[int]
    total_financial_commitment: Optional[str]
    prize_money_details: Optional[str]
    scores_url: Optional[str]
    draws_url: Optional[str]
    schedule_url: Optional[str]
    tournament_site_url: Optional[str]
    overview_url: Optional[str]
    tickets_url: Optional[str]
    ticket_hotline: Optional[str]
    phone_number: Optional[str]
    email: Optional[str]
    pdf_schedule_url: Optional[str]
    pdf_mds_url: Optional[str]
    pdf_mdd_url: Optional[str]
    pdf_qs_url: Optional[str]
    is_live: Optional[bool]
    is_past_event: Optional[bool]

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class TournamentOverviewRow:
    snapshot_ts_utc: str
    tournament_id: str
    sponsor_title: Optional[str]
    bio: Optional[str]
    singles_draw_size: Optional[int]
    doubles_draw_size: Optional[int]
    surface: Optional[str]
    surface_sub_cat: Optional[str]
    in_outdoor: Optional[str]
    prize: Optional[str]
    total_financial_commitment: Optional[str]
    location: Optional[str]
    website_url: Optional[str]
    event_type: Optional[str]
    event_type_detail: Optional[int]
    facebook_url: Optional[str]
    twitter_url: Optional[str]
    instagram_url: Optional[str]
    raw_overview_json: Optional[Dict[str, Any]]

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class TopSeedRow:
    snapshot_ts_utc: str
    tournament_id: str
    event_year: int
    seed_number: Optional[int]
    full_name: Optional[str]
    win_loss: Optional[str]
    best_finish: Optional[str]
    player_profile_url: Optional[str]
    player_country_url: Optional[str]

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class HeadToHeadMatchRow:
    snapshot_ts_utc: str
    left_player_id: str
    right_player_id: str
    event_id: Optional[str]
    event_year: Optional[int]
    tournament_name: Optional[str]
    surface: Optional[str]
    in_outdoor_display: Optional[str]
    match_id: Optional[str]
    winner_player_id: Optional[str]
    is_doubles: Optional[bool]
    is_qualifier: Optional[bool]
    round_short_name: Optional[str]
    round_long_name: Optional[str]
    match_time: Optional[str]
    is_match_live: Optional[bool]

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class MatchScheduleRow:
    snapshot_ts_utc: str
    tournament_slug: str
    tournament_id: str
    day: Optional[str]
    court_name: Optional[str]
    schedule_type: Optional[str]
    start_label: Optional[str]
    player_1_name: Optional[str]
    player_1_profile_url: Optional[str]
    player_1_seed: Optional[str]
    player_2_name: Optional[str]
    player_2_profile_url: Optional[str]
    player_2_seed: Optional[str]
    status_text: Optional[str]

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class MatchResultRow:
    snapshot_ts_utc: str
    tournament_slug: str
    tournament_id: str
    day_label: Optional[str]
    round_and_court: Optional[str]
    match_duration: Optional[str]
    player_1_name: Optional[str]
    player_1_profile_url: Optional[str]
    player_1_is_winner: Optional[bool]
    player_1_scores: Optional[str]
    player_2_name: Optional[str]
    player_2_profile_url: Optional[str]
    player_2_is_winner: Optional[bool]
    player_2_scores: Optional[str]
    h2h_url: Optional[str]
    stats_url: Optional[str]
    umpire: Optional[str]
    match_notes: Optional[str]

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)
