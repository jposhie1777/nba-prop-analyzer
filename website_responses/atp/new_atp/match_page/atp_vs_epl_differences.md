# ATP vs EPL Scraper Differences

## oddspedia_client.py changes needed for ATP

### getMatchInfo
- EPL: GET getMatchInfo?matchKey={mk}&geoCode=US&wettsteuer=0&r=wv  → 400
- ATP: GET getMatchInfo?matchKey={mk}&geoCode=US&language=us         → 200
- Remove r=wv and wettsteuer params for tennis

### getPerMatchStats
- EPL: 200 ✅
- ATP: 404 ❌ — skip entirely

### getLeagueStandings
- EPL: 200 ✅
- ATP: skip — has_standings=0 in getMatchInfo

### bettingStats structure
- EPL: hasTabs=true, nested categories (goals, corners, etc.)
- ATP: hasTabs=false, flat single data object, stats are games/sets percentages

### H2H periods
- EPL: periods are halftime/fulltime scores
- ATP: periods are per-set scores with tiebreak
  Format: [{"period_type":"set","period_number":1,"home":6,"away":4,"tiebreak":null}]

### lastMatches status field
- EPL: status = period scores for soccer
- ATP: status = set scores JSON string (same field, tennis content)

### No draws
- EPL: outcome can be w/d/l
- ATP: outcome is only w/l

### Tennis-specific fields in getMatchInfo
- surface: "hard" / "clay" / "grass"
- prize_money: "8995555"
- prize_currency: "$"
- ht_rank: "ATP 41"
- at_rank: "ATP 82"
- round_name: "Round Of 128"
- venue fields often null for tennis

## scraper.scrape() kwargs for ATP
scraper.scrape(
    url,
    league_category="usa",
    league_slug="atp-miami",
    season_id=134091,
    sport="tennis",
)

## BigQuery schema additions for ATP (vs EPL)
- surface STRING
- prize_money STRING
- prize_currency STRING  
- ht_rank STRING
- at_rank STRING
- sets_home INT64
- sets_away INT64

## market_groups for ATP Miami
From getEventStage default_market_groups: [3, 1, 2, 4]
- 1: match winner (moneyline)
- 2: set betting  
- 3: total games
- 4: handicap