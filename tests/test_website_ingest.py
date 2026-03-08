from pathlib import Path

from mobile_api.ingest.atp.website_ingest import _extract_h2h_ids, _extract_slug_and_tournament_id, _load_capture_file


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
