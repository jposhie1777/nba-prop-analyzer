# Oddspedia MLS capture assessment

This assessment was run against `website_responses/mls` (there is no `website_responses/oddspedia/mls` directory in the repo).

## Direct answers

- **Can we scrape all listed odds markets?** Yes.
  - `1x2`, `BTTS`, `Draw No Bet`, `Double Chance`, `European Handicap`, and `Total Corners` all contain API JSON responses that can be parsed from the saved files.
- **Can we scrape Match Info (including Market Keys / betting insights)?** Yes.
  - `match_info` contains a full JSON payload with 100+ fields and `data.match_keys` statements.
- **Can we scrape the statistics section for compiling stats?** Yes, with caveats.
  - The local `statistics_extract` file includes a captured token list from the insights page DOM that can be parsed.
  - For robust production extraction, use page-tab aware scraping (Total/H2H × Goals/BTTS/Corners/Cards) and map DOM elements to structured fields.

## Local proof-of-parse

Run:

```bash
python analyze_mls_capture.py
```

Output from this snapshot:

- 1x2: 9 normalized odds rows
- BTTS: 4 rows
- Draw No Bet: 2 rows
- Double Chance: 3 rows
- European Handicap: 3 rows
- Total Corners: 10 rows
- Match info fields: 106
- Match keys: 28
- Statistics tokens: 436

Structured extraction is written to:

- `website_responses/mls/extracted_report.json`

## Notes for scaling this

1. **Use only response body**, not full devtools header dumps, as raw storage format.
2. **Track `matchId`, `marketGroupId`, `period id`, and line name (`main` / alternative handicap lines)**.
3. **Persist Match Keys separately** as timestamped insights text (`statement`, team ids).
4. **For statistics tabs**, scrape each tab state independently and emit tidy records (`metric`, `team`, `value`, `scope=Total|H2H`, `subtab=Goals|BTTS|Corners|Cards`).
