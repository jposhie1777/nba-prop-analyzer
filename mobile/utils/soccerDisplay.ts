import type { SoccerLeague, SoccerStandingRow } from "@/hooks/soccer/useSoccerMatchups";

const LEAGUE_LOGO_FOLDER: Record<SoccerLeague, string> = {
  epl: "England - Premier League",
  mls: "USA - MLS",
};

const TEAM_ALIASES: Record<SoccerLeague, Record<string, string>> = {
  epl: {
    "man utd": "Manchester United",
    "man city": "Manchester City",
    "spurs": "Tottenham Hotspur",
    "wolves": "Wolverhampton Wanderers",
    "newcastle": "Newcastle United",
    "nottm forest": "Nottingham Forest",
    "brighton": "Brighton & Hove Albion",
    "west ham": "West Ham United",
    "ipswich": "Ipswich Town",
    "leicester": "Leicester City",
  },
  mls: {
    "st. louis": "Saint Louis City",
    "st louis": "Saint Louis City",
    "d.c.": "DC United",
    "new york": "New York Red Bulls",
  },
};

function normalizeTeamName(value?: string | null): string {
  return (value ?? "")
    .toLowerCase()
    .replace(/&/g, " and ")
    .replace(/[^a-z0-9]+/g, " ")
    .trim();
}

export function getSoccerTeamLogoUrl(league: SoccerLeague, teamName?: string | null): string | null {
  if (!teamName) return null;
  const normalized = normalizeTeamName(teamName);
  const alias = TEAM_ALIASES[league][normalized];
  const canonicalTeam = alias ?? teamName;
  const leaguePath = encodeURIComponent(LEAGUE_LOGO_FOLDER[league]);
  const teamPath = encodeURIComponent(canonicalTeam);
  return `https://raw.githubusercontent.com/luukhopman/football-logos/master/logos/${leaguePath}/${teamPath}.png`;
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
