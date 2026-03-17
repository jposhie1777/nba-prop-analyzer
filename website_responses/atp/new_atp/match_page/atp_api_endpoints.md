# ATP Oddspedia API Reference

## Identity
- sport: tennis
- sport_id: 5
- category_slug: usa (for ATP Miami)
- league_slug: atp-miami
- league_id: 2490
- season_id: 134091
- listing_url: https://www.oddspedia.com/us/tennis/usa/atp-miami

## Match Page URL Format
https://oddspedia.com/us/tennis/{ht_slug}-{at_slug}-{match_key}
Example: https://oddspedia.com/us/tennis/alejandro-tabilo-francisco-comesana-1131703

## Confirmed Working Endpoints

### getMatchInfo
GET https://oddspedia.com/api/v1/getMatchInfo?matchKey={mk}&geoCode=US&language=us
- NOTE: do NOT include r=wv — causes 400
- Returns: full match data, player slugs, rankings, surface, prize money, round

### getEventStage
GET https://oddspedia.com/api/v1/getEventStage?matchKey={mk}&geoCode=US&geoState=NY&language=us
- Returns: matchstatus, default_market_groups [3,1,2,4], has_odds, has_sr_coverage

### getMatchBettingStats
GET https://oddspedia.com/api/v1/getMatchBettingStats?matchKey={mk}&language=us
- Returns: over/under games %, over/under sets %
- hasTabs: false (flat structure, no sub-tabs like EPL)
- total_matches: {home: N, away: N}

### getHeadToHead
GET https://oddspedia.com/api/v1/getHeadToHead?matchKey={mk}&all=1&language=us
- Returns: ht_wins, at_wins, draws (null for tennis), played_matches, period
- matches[]: id, starttime, ht/at/slugs/ids, hscore/ascore (sets), periods (JSON string of set scores), winner

### getTeamLastMatches (home player)
GET https://oddspedia.com/api/v1/getTeamLastMatches?matchKey={mk}&type=home&teamId=0&upcomingMatchesLimit=2&finishedMatchesLimit=5&geoCode=US&language=us
- Returns: keyed by player_id e.g. {"24622": {matches:[], leagues:[], ht_form, at_form}}
- matches[]: outcome (w/l only, no draws), home bool, status (set scores JSON), matchstatus, league_id

### getTeamLastMatches (away player)
GET https://oddspedia.com/api/v1/getTeamLastMatches?matchKey={mk}&type=away&teamId=0&upcomingMatchesLimit=2&finishedMatchesLimit=5&geoCode=US&language=us

### getMatchMaxOddsByGroup
GET https://oddspedia.com/api/v1/getMatchMaxOddsByGroup?matchId={matchId}&marketGroupId={N}&inplay=0&geoCode=US&geoState=NY&language=us
- NOTE: uses matchId (numeric, from getMatchInfo.data.id) NOT match_key
- default_market_groups for ATP Miami: [3, 1, 2, 4]
- marketGroupId=1: match winner (moneyline)
- marketGroupId=2: set betting
- marketGroupId=3: total games
- marketGroupId=4: handicap

## NOT Available for Tennis
- getPerMatchStats → 404
- getLeagueStandings → has_standings=0, skip
- getMatchLineUps → untested but likely N/A
- getMatchInfo with r=wv → 400

## bettingStats Structure (tennis-specific, flat)
{
  "hasTabs": false,
  "data": {
    "hasTabs": false,
    "label": "total",
    "data": [
      {"label": "over_under_games_percentage", "home": 62, "away": 60, "value": "20.5"},
      {"label": "over_under_games_percentage", "home": 58, "away": 55, "value": "21.5"},
      {"label": "over_under_sets_percentage", "home": 47, "away": 33, "value": "2.5"}
    ],
    "total_matches": {"home": 71, "away": 56}
  }
}