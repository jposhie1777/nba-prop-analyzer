import type { SoccerLeague, SoccerStandingRow } from "@/hooks/soccer/useSoccerMatchups";

export const TEAM_ALIASES: Record<SoccerLeague, Record<string, string>> = {
  epl: {
    "afc bournemouth": "Bournemouth",
    "arsenal fc": "Arsenal",
    "brentford fc": "Brentford",
    "brighton hove albion": "Brighton and Hove Albion",
    "brighton and hove albion": "Brighton and Hove Albion",
    "burnley fc": "Burnley",
    "chelsea fc": "Chelsea",
    "everton fc": "Everton",
    "fulham fc": "Fulham",
    "liverpool fc": "Liverpool",
    "man utd": "Manchester United",
    "man city": "Manchester City",
    "manchester united fc": "Manchester United",
    "manchester city fc": "Manchester City",
    "spurs": "Tottenham Hotspur",
    "tottenham hotspur fc": "Tottenham Hotspur",
    "sunderland afc": "Sunderland",
    "wolves": "Wolverhampton Wanderers",
    "newcastle": "Newcastle United",
    "nottm forest": "Nottingham Forest",
    "brighton": "Brighton and Hove Albion",
    "west ham": "West Ham United",
    "ipswich": "Ipswich Town",
    "leicester": "Leicester City",
  },
  mls: {
    "cf montreal": "CF Montréal",
    "montreal impact": "CF Montréal",
    "st. louis": "St. Louis City SC",
    "st louis": "St. Louis City SC",
    "saint louis city": "St. Louis City SC",
    "saint louis city sc": "St. Louis City SC",
    "new york city": "New York City FC",
    "new york city fc": "New York City FC",
    "new york red bulls": "New York Red Bulls",
    "sporting kansas city": "Sporting Kansas City",
    "la galaxy": "LA Galaxy",
    "los angeles galaxy": "LA Galaxy",
    "los angeles fc": "Los Angeles FC",
    "fc dallas": "FC Dallas",
    "fc cincinnati": "FC Cincinnati",
    "inter miami": "Inter Miami CF",
    "inter miami cf": "Inter Miami CF",
    "san jose earthquakes": "San Jose Earthquakes",
    "new england revolution": "New England Revolution",
    "d.c.": "DC United",
    "dc united": "DC United",
    "toronto fc": "Toronto FC",
    "columbus crew": "Columbus Crew",
    "philadelphia union": "Philadelphia Union",
    "chicago fire": "Chicago Fire",
    "orlando city": "Orlando City",
    "atlanta united": "Atlanta United",
    "charlotte fc": "Charlotte FC",
    "nashville fc": "Nashville SC",
    "nashville sc": "Nashville SC",
    "houston dynamo": "Houston Dynamo",
    "colorado rapids": "Colorado Rapids",
    "austin fc": "Austin FC",
    "vancouver whitecaps": "Vancouver Whitecaps FC",
    "vancouver whitecaps fc": "Vancouver Whitecaps FC",
    "real salt lake": "Real Salt Lake",
    "seattle sounders": "Seattle Sounders FC",
    "seattle sounders fc": "Seattle Sounders FC",
    "portland timbers": "Portland Timbers",
    "new york": "New York Red Bulls",
  },
};

export function normalizeTeamName(value?: string | null): string {
  return (value ?? "")
    .toLowerCase()
    .replace(/&/g, " and ")
    .replace(/[éèêë]/g, "e")
    .replace(/[^a-z0-9]+/g, " ")
    .trim();
}

function rowRecordValue(row: SoccerStandingRow): string | null {
  if (row.win_loss_record) return String(row.win_loss_record);
  if (row.standing_note) return String(row.standing_note);

  if (
    typeof row.wins === "number" &&
    typeof row.losses === "number" &&
    typeof row.draws === "number"
  ) {
    return `${row.wins}-${row.losses}-${row.draws}`;
  }
  return null;
}

export function buildRecordMap(rows: SoccerStandingRow[]): Map<string, string> {
  const map = new Map<string, string>();
  rows.forEach((row) => {
    const key = normalizeTeamName(row.team_name);
    const record = rowRecordValue(row);
    if (key && record) {
      map.set(key, record);
    }
  });
  return map;
}

export function resolveRecordForTeam(
  league: SoccerLeague,
  teamName: string | null | undefined,
  recordMap: Map<string, string>
): string {
  if (!teamName) return "-";

  const normalized = normalizeTeamName(teamName);
  const alias = TEAM_ALIASES[league][normalized];
  const key = normalizeTeamName(alias ?? teamName);

  if (recordMap.has(key)) {
    return recordMap.get(key) ?? "-";
  }

  for (const [candidate, record] of recordMap.entries()) {
    if (candidate.includes(key) || key.includes(candidate)) {
      return record;
    }
  }
  return "-";
}

export type SportsDbTeam = {
  strTeam?: string | null;
  strTeamShort?: string | null;
  strBadge?: string | null;
};

export function buildBadgeMapFromSportsDbRows(rows: SportsDbTeam[]): Map<string, string> {
  const map = new Map<string, string>();

  rows.forEach((row) => {
    const badge = row.strBadge ?? null;
    if (!badge) return;

    const candidates = [row.strTeam, row.strTeamShort]
      .map((value) => normalizeTeamName(value))
      .filter(Boolean);

    candidates.forEach((key) => map.set(key, badge));
  });

  return map;
}

export function resolveBadgeForTeam(
  league: SoccerLeague,
  teamName: string | null | undefined,
  badgeMap: Map<string, string>
): string | null {
  if (!teamName) return null;

  const normalized = normalizeTeamName(teamName);
  const aliased = TEAM_ALIASES[league][normalized];
  const key = normalizeTeamName(aliased ?? teamName);

  if (badgeMap.has(key)) return badgeMap.get(key) ?? null;

  for (const [candidate, badge] of badgeMap.entries()) {
    if (candidate.includes(key) || key.includes(candidate)) {
      return badge;
    }
  }

  return null;
}

const ODDS_TEAM_CODE_OVERRIDES: Record<string, string> = {
  "manchester united": "MUN",
  "manchester city": "MCI",
  "new york red bulls": "NYR",
  "new york city fc": "NYC",
  "los angeles fc": "LAFC",
  "la galaxy": "LAG",
  "tottenham hotspur": "TOT",
  "west ham united": "WHU",
  "newcastle united": "NEW",
  "nottingham forest": "NFO",
  "wolverhampton wanderers": "WOL",
  "brighton and hove albion": "BHA",
  "st louis city sc": "STL",
  "sporting kansas city": "SKC",
  "real salt lake": "RSL",
  "san jose earthquakes": "SJ",
  "new england revolution": "NE",
  "inter miami cf": "MIA",
  "columbus crew": "CLB",
};

export function buildTeamOddsCode(teamName?: string | null): string {
  const normalized = normalizeTeamName(teamName);
  if (!normalized) return "TEAM";
  if (ODDS_TEAM_CODE_OVERRIDES[normalized]) return ODDS_TEAM_CODE_OVERRIDES[normalized];

  const tokens = normalized.split(" ").filter(Boolean);
  const filtered = tokens.filter((token) => !["fc", "cf", "sc", "afc"].includes(token));
  const base = filtered.length ? filtered : tokens;
  if (!base.length) return "TEAM";

  const [first, second, third] = base;
  if (first === "new" && second) return `N${second[0]}`.toUpperCase();
  if ((first === "los" || first === "la") && second) return `LA${second[0]}`.toUpperCase();

  if (first.length >= 3) return first.slice(0, 3).toUpperCase();

  const initials = [first, second, third]
    .filter(Boolean)
    .map((token) => token[0])
    .join("")
    .toUpperCase();

  if (initials.length >= 2) return initials;
  return first.toUpperCase();
}
