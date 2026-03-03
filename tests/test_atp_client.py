from atp_client import ATPClient


class DummyResponse:
    def __init__(self, status_code=200, payload=None, text="", headers=None):
        self.status_code = status_code
        self._payload = payload or {}
        self.text = text
        self.headers = headers or {"Content-Type": "application/json"}

    def raise_for_status(self):
        if self.status_code >= 400:
            raise RuntimeError("http error")

    def json(self):
        return self._payload


class DummySession:
    def __init__(self, response):
        self.response = response

    def get(self, url, timeout, headers):
        return self.response


def test_uses_cached_payload_on_304():
    client = ATPClient()
    client._session = DummySession(DummyResponse(status_code=304, headers={}))
    result = client.fetch_calendar(
        cache_hints={"etag": '"abc"', "last_modified": "Tue, 03 Mar 2026 12:17:05 GMT", "cached_payload": {"TournamentDates": []}}
    )
    assert result["is_not_modified"] is True
    assert result["fetched_json"] == {"TournamentDates": []}


def test_raises_on_304_without_cache_payload():
    client = ATPClient()
    client._session = DummySession(DummyResponse(status_code=304, headers={}))
    try:
        client.fetch_calendar(cache_hints={"etag": '"abc"'})
        raised = False
    except RuntimeError:
        raised = True
    assert raised is True


def test_fetch_match_results_html_supports_304_cached_text():
    client = ATPClient()
    client._session = DummySession(DummyResponse(status_code=304, headers={}))
    result = client.fetch_match_results_html(
        tournament_slug="indian-wells",
        tournament_id="404",
        cache_hints={"cached_payload": "<html>cached</html>"},
    )
    assert result["fetched_text"] == "<html>cached</html>"
