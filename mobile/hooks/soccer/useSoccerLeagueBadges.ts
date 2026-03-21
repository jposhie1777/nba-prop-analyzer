import { useEffect, useMemo, useState } from "react";

import type { SoccerLeague } from "@/hooks/soccer/useSoccerMatchups";
import {
  buildBadgeMapFromSportsDbRows,
  type SportsDbTeam,
} from "@/utils/soccerDisplay";

const SPORTS_DB_LEAGUE_NAME: Record<SoccerLeague, string> = {
  epl: "English Premier League",
  mls: "American Major League Soccer",
};

const badgeCache = new Map<SoccerLeague, Map<string, string>>();

export function useSoccerLeagueBadges(league: SoccerLeague) {
  const [data, setData] = useState<Map<string, string> | null>(badgeCache.get(league) ?? null);
  const [loading, setLoading] = useState<boolean>(!badgeCache.has(league));

  useEffect(() => {
    let cancelled = false;
    const cached = badgeCache.get(league);
    if (cached) {
      setData(cached);
      setLoading(false);
      return;
    }

    async function run() {
      try {
        setLoading(true);
        const leagueName = SPORTS_DB_LEAGUE_NAME[league];
        const url = `https://www.thesportsdb.com/api/v1/json/3/search_all_teams.php?l=${encodeURIComponent(leagueName)}`;
        const res = await fetch(url);
        if (!res.ok) {
          throw new Error(`HTTP ${res.status}`);
        }
        const json = (await res.json()) as { teams?: SportsDbTeam[] };
        const badgeMap = buildBadgeMapFromSportsDbRows(json.teams ?? []);
        badgeCache.set(league, badgeMap);
        if (!cancelled) setData(badgeMap);
      } catch {
        if (!cancelled) setData(new Map());
      } finally {
        if (!cancelled) setLoading(false);
      }
    }

    run();
    return () => {
      cancelled = true;
    };
  }, [league]);

  return useMemo(
    () => ({
      data: data ?? new Map<string, string>(),
      loading,
    }),
    [data, loading]
  );
}
