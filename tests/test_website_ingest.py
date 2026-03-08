from pathlib import Path

from mobile_api.ingest.atp.website_ingest import (
    _extract_h2h_ids,
    _extract_slug_and_tournament_id,
    _extract_daily_schedule_time_fields,
    _flatten_html_payload,
    _load_capture_file,
)


def test_load_capture_file_json_extracts_payload():
    capture = _load_capture_file(Path("website_responses/atp/who_is_playing"))

    assert capture["content_type"] == "application/json"
    assert capture["request_url"].endswith("/en/-/tournaments/404/whoisplaying")
    assert "PlayersList" in capture["payload_json"]


def test_load_capture_file_html_extracts_payload():
    capture = _load_capture_file(Path("website_responses/atp/daily_schedule"))

    assert capture["content_type"] == "text/html"
    assert capture["request_url"].endswith("/en/scores/current/indian-wells/404/daily-schedule")
    assert "<!doctype html>" in capture["payload_text"]


def test_url_extractors():
    slug, tournament_id = _extract_slug_and_tournament_id(
        "https://www.atptour.com/en/scores/current/indian-wells/404/results"
    )
    left_id, right_id = _extract_h2h_ids("https://www.atptour.com/en/-/www/h2h/m0ni/gc88")

    assert (slug, tournament_id) == ("indian-wells", "404")
    assert (left_id, right_id) == ("M0NI", "GC88")


def test_flatten_html_payload_extracts_text_and_links():
    html = '<div><a href="/en/test">ATP Link</a><span>Round of 32</span></div>'

    text_chunks, links = _flatten_html_payload(html)

    assert any("ATP Link" in chunk for chunk in text_chunks)
    assert any("Round of 32" in chunk for chunk in text_chunks)
    assert links[0]["href"] == "/en/test"
    assert links[0]["text"] == "ATP Link"


def test_daily_schedule_time_field_extraction():
    capture = _load_capture_file(Path("website_responses/atp/daily_schedule"))

    start_times, not_before_times, schedule_items = _extract_daily_schedule_time_fields(capture["payload_text"])

    assert any(item.startswith("Starts At") for item in start_times)
    assert any(item.startswith("Not Before") for item in not_before_times)
    assert any(item.get("time_type") == "starts_at" for item in schedule_items)
    assert any(item.get("time_type") == "not_before" for item in schedule_items)


def test_extract_player_id_from_profile_url():
    from mobile_api.ingest.atp.website_ingest import _extract_player_id_from_profile_url

    assert _extract_player_id_from_profile_url("/en/players/jakub-mensik/m0ni/overview") == "M0NI"
    assert _extract_player_id_from_profile_url("https://www.atptour.com/en/players/marcos-giron/gc88/overview") == "GC88"
    assert _extract_player_id_from_profile_url(None) is None


def test_build_h2h_pairs_from_schedule_rows_dedupes_pairs():
    from mobile_api.ingest.atp.website_ingest import _build_h2h_pairs_from_schedule_rows

    rows = [
        {
            "player_1_profile_url": "/en/players/jakub-mensik/m0ni/overview",
            "player_2_profile_url": "/en/players/marcos-giron/gc88/overview",
        },
        {
            "player_1_profile_url": "/en/players/marcos-giron/gc88/overview",
            "player_2_profile_url": "/en/players/jakub-mensik/m0ni/overview",
        },
        {
            "player_1_profile_url": "/en/players/carlos-alcaraz/a0e2/overview",
            "player_2_profile_url": "/en/players/jakub-mensik/m0ni/overview",
        },
    ]

    pairs = _build_h2h_pairs_from_schedule_rows(rows)

    assert pairs == [("M0NI", "GC88"), ("A0E2", "M0NI")]
