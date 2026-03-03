from atp_normalize import (
    normalize_calendar,
    normalize_head_to_head,
    normalize_match_results_html,
    normalize_match_schedule_html,
    normalize_overview,
    normalize_top_seeds,
)


def test_normalize_calendar_extracts_months_and_tournaments():
    sample = {
        "TournamentDates": [
            {
                "DisplayDate": "January, 2026",
                "IsExpanded": False,
                "NoEvents": 1,
                "Tournaments": [{"Id": "9900", "Name": "United Cup", "QualySinglesDrawPrintUrl": "x"}],
            }
        ]
    }
    months, tournaments = normalize_calendar(sample, snapshot_ts_utc="2026-01-01T00:00:00+00:00")
    assert len(months) == 1
    assert months[0].is_expanded is False
    assert tournaments[0].pdf_qs_url == "x"


def test_normalize_overview_with_raw_payload():
    row = normalize_overview(
        tournament_id="4999",
        overview_json={"SponsorTitle": "Acme Open", "FbLink": "https://facebook.com/acme"},
        snapshot_ts_utc="2026-01-01T00:00:00+00:00",
        include_raw_json=True,
    )
    assert row.tournament_id == "4999"
    assert row.facebook_url == "https://facebook.com/acme"


def test_normalize_top_seeds_and_h2h():
    top_seed_rows = normalize_top_seeds("404", 2025, {"SinglePlayers": [{"SeedNumber": 1}]}, snapshot_ts_utc="2026-01-01T00:00:00+00:00")
    assert len(top_seed_rows) == 1

    h2h_rows = normalize_head_to_head(
        "C0E9",
        "TD51",
        {"Tournaments": [{"EventId": "807", "MatchResults": [{"MatchId": "MS001", "Round": {"LongName": "Final"}}]}]},
        snapshot_ts_utc="2026-01-01T00:00:00+00:00",
    )
    assert len(h2h_rows) == 1
    assert h2h_rows[0].round_long_name == "Final"


def test_normalize_match_schedule_html():
    html = '''
    <h4 class="day">Mon, 02 March, 2026 <span>(Day 2)</span></h4>
    <div class="schedule" data-matchdate="2026-03-02">
      <div class="schedule-header"><div class="schedule-location-timestamp"><span><strong>Stadium 3</strong></span><span class="matchtime">Starts At 10:00</span></div><div class="schedule-type">Q1</div></div>
      <div class="schedule-content"><div class="schedule-players"><div class="player"><div class="name"><a href="/en/players/p1/aaa/overview"><span>S.</span> Shimabukuro</a><div class="rank"><span>(22)</span></div></div></div><div class="status">Defeats</div><div class="opponent"><div class="name"><a href="/en/players/p2/bbb/overview">Colton Smith</a></div></div></div></div>
    </div>
    '''
    rows = normalize_match_schedule_html("indian-wells", "404", html, snapshot_ts_utc="2026-01-01T00:00:00+00:00")
    assert len(rows) == 1
    assert rows[0].court_name == "Stadium 3"
    assert rows[0].player_1_profile_url == "/en/players/p1/aaa/overview"


def test_normalize_match_results_html():
    html = '''
    <div class="tournament-day"><h4>Mon, 02 March, 2026 <span>Day (2)</span></h4></div>
    <div class="match">
      <div class="match-header"><span><strong>1st Round Qualifying - Stadium 5</strong></span><span>01:44:19</span></div>
      <div class="match-content"><div class="match-stats">
        <div class="stats-item"><div class="player-info"><div class="name"><a href="/en/players/vit-kopriva/ki82/overview">Vit Kopriva</a></div><div class="winner"><span class="icon-checkmark"></span></div></div><div class="scores"><div class="score-item"><span>6</span></div></div></div>
        <div class="stats-item"><div class="player-info"><div class="name"><a href="/en/players/rei-sakamoto/s0uv/overview">Rei Sakamoto</a></div></div><div class="scores"><div class="score-item"><span>4</span></div></div></div>
      </div></div>
      <div class="match-footer"><div class="match-umpire">Ump: Chase Urban</div><div class="match-cta"><a href="/h2h">H2H</a><a href="/stats">Stats</a></div></div>
      <div class="match-notes">Game Set and Match Vit Kopriva.</div>
    </div>
    '''
    rows = normalize_match_results_html("indian-wells", "404", html, snapshot_ts_utc="2026-01-01T00:00:00+00:00")
    assert len(rows) == 1
    assert rows[0].round_and_court == "1st Round Qualifying - Stadium 5"
    assert rows[0].h2h_url == "/h2h"
