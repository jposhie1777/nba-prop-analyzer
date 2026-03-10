from pathlib import Path

from mobile_api.ingest.kbo.backfill import build_team_summary, parse_games_from_html


def test_parse_games_from_snapshot():
    html = Path("website_responses/kbo/daily_schedule").read_text(encoding="utf-8")
    games = parse_games_from_html(html)

    assert len(games) > 20

    first = games[0]
    assert first["game_date"] == "2025-10-01"
    assert first["away_team"] == "NC"
    assert first["home_team"] == "LG"
    assert first["away_runs"] == 7
    assert first["home_runs"] == 3
    assert first["status"] == "final"



def test_team_summary_has_core_metrics():
    html = Path("website_responses/kbo/daily_schedule").read_text(encoding="utf-8")
    games = parse_games_from_html(html)
    summary = build_team_summary(games)

    assert len(summary) > 0
    sample = summary[0]

    for key in (
        "season",
        "team",
        "games_played",
        "wins",
        "losses",
        "ties",
        "runs_scored",
        "runs_allowed",
        "avg_runs_scored",
        "avg_runs_allowed",
    ):
        assert key in sample
