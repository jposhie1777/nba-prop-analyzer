const TEAM_ABBR_BY_NAME: Record<string, string> = {
  "Arizona Diamondbacks": "ari",
  "Atlanta Braves": "atl",
  "Baltimore Orioles": "bal",
  "Boston Red Sox": "bos",
  "Chicago Cubs": "chc",
  "Chicago White Sox": "chw",
  "Cincinnati Reds": "cin",
  "Cleveland Guardians": "cle",
  "Colorado Rockies": "col",
  "Detroit Tigers": "det",
  "Houston Astros": "hou",
  "Kansas City Royals": "kc",
  "Los Angeles Angels": "laa",
  "Los Angeles Dodgers": "lad",
  "Miami Marlins": "mia",
  "Milwaukee Brewers": "mil",
  "Minnesota Twins": "min",
  "New York Mets": "nym",
  "New York Yankees": "nyy",
  "Athletics": "oak",
  "Philadelphia Phillies": "phi",
  "Pittsburgh Pirates": "pit",
  "San Diego Padres": "sd",
  "San Francisco Giants": "sf",
  "Seattle Mariners": "sea",
  "St. Louis Cardinals": "stl",
  "Tampa Bay Rays": "tb",
  "Texas Rangers": "tex",
  "Toronto Blue Jays": "tor",
  "Washington Nationals": "wsh",
};

const TEAM_ID_BY_NAME: Record<string, number> = {
  "Arizona Diamondbacks": 109,
  "Atlanta Braves": 144,
  "Baltimore Orioles": 110,
  "Boston Red Sox": 111,
  "Chicago Cubs": 112,
  "Chicago White Sox": 145,
  "Cincinnati Reds": 113,
  "Cleveland Guardians": 114,
  "Colorado Rockies": 115,
  "Detroit Tigers": 116,
  "Houston Astros": 117,
  "Kansas City Royals": 118,
  "Los Angeles Angels": 108,
  "Los Angeles Dodgers": 119,
  "Miami Marlins": 146,
  "Milwaukee Brewers": 158,
  "Minnesota Twins": 142,
  "New York Mets": 121,
  "New York Yankees": 147,
  "Athletics": 133,
  "Philadelphia Phillies": 143,
  "Pittsburgh Pirates": 134,
  "San Diego Padres": 135,
  "San Francisco Giants": 137,
  "Seattle Mariners": 136,
  "St. Louis Cardinals": 138,
  "Tampa Bay Rays": 139,
  "Texas Rangers": 140,
  "Toronto Blue Jays": 141,
  "Washington Nationals": 120,
};

export function getMlbTeamLogoByName(teamName?: string | null): string | null {
  const key = (teamName ?? "").trim();
  if (!key) return null;
  const abbr = TEAM_ABBR_BY_NAME[key];
  if (abbr) return `https://a.espncdn.com/i/teamlogos/mlb/500/${abbr}.png`;
  const teamId = TEAM_ID_BY_NAME[key];
  if (teamId) return `https://www.mlbstatic.com/team-logos/${teamId}.svg`;
  return null;
}

export const getMlbTeamLogo = getMlbTeamLogoByName;
