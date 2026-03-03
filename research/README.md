# Research Toolkit: API Endpoint Discovery & Inspection

This folder contains a modular Python toolkit for discovering and inspecting API endpoints used by sports sites (ATP, PGA, NHL, La Liga, etc.) in a low-volume, research-oriented workflow.

## Folder Structure

```text
research/
├── README.md
├── requirements.txt
├── __init__.py
├── inspect_requests.py
├── network_probe.py
├── endpoint_scanner.py
└── config/
    ├── headers.json
    └── endpoints.txt
```

## Setup (GitHub Codespaces)

From the repository root:

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r research/requirements.txt
python -m playwright install chromium
```

## Tooling Workflows

### 1) Capture browser network traffic

Use Playwright to open a site, print outgoing requests, and store them as JSON.

```bash
python research/network_probe.py \
  --url https://www.atptour.com \
  --out-json research/output/atp_requests.json
```

### 2) Capture and save HAR files

Add `--har` to generate a HAR archive for deeper analysis:

```bash
python research/network_probe.py \
  --url https://www.atptour.com \
  --out-json research/output/atp_requests.json \
  --har research/output/atp_capture.har
```

This creates:
- Request event log JSON (easy to script against)
- HAR file (loadable in browser devtools and HAR analyzers)

### 3) Replay cURL or saved requests

Replay a copied cURL command:

```bash
python research/inspect_requests.py \
  --curl "curl 'https://example.com/api' -H 'accept: application/json'"
```

Or replay a saved request JSON:

```bash
python research/inspect_requests.py --request-file research/output/sample_request.json
```

Expected output includes:
- HTTP status code
- content type
- lightweight inferred output schema (for JSON responses)
- body preview

### 4) Scan candidate endpoints

Edit `research/config/endpoints.txt` to include candidate API URLs, then run:

```bash
python research/endpoint_scanner.py \
  --endpoints-file research/config/endpoints.txt \
  --headers-config research/config/headers.json \
  --out-json research/output/scan_results.json
```

This sends one bounded request per endpoint and records status/type/length.

## Notes on Safety & Scope

- This toolkit is designed for **endpoint discovery and response inspection**, not high-volume scraping.
- Keep request rates low and stay within each provider's terms.
- Start with public pages and browser-observed calls before deeper probing.

## Example Request JSON Format

```json
{
  "method": "GET",
  "url": "https://example.com/api/v1/data",
  "headers": {
    "Accept": "application/json"
  },
  "data": null
}
```
