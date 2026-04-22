// hooks/useNbaResearch.ts
import { useCallback, useEffect, useState } from "react";
import { API_BASE } from "@/lib/config";

export type ResearchCategory = { value: string; label: string };

export type ResearchGame = {
  game_id: string;
  away_team_code: string;
  home_team_code: string;
  date_label: string;
  time_label: string;
  sort_key: string;
  home_spread_line: string | null;
  away_spread_line: string | null;
  favorite_label: string;
  total_line: number | null;
};

export type ResearchProp = {
  prop_id: string;
  player_id: string;
  player_name: string;
  position: string | null;
  team_code: string | null;
  opp_team_code: string | null;
  game_id: string | null;
  is_home: boolean | null;
  injury_status: string | null;
  category: string;
  line: number | null;
  over_under: string;
  is_alternate: boolean;
  pf_rating: number | null;
  streak: number | null;
  matchup_rank: number | null;
  matchup_value: number | null;
  matchup_label: string | null;
  hit_rate_season: number | null;
  hit_rate_season_raw: string | null;
  hit_rate_vs_team: number | null;
  hit_rate_vs_team_raw: string | null;
  hit_rate_l5: number | null;
  hit_rate_l5_raw: string | null;
  hit_rate_l10: number | null;
  hit_rate_l10_raw: string | null;
  hit_rate_l20: number | null;
  hit_rate_l20_raw: string | null;
  avg_l10: number | null;
  avg_home_away: number | null;
  avg_vs_opponent: number | null;
  dk_price: number | null;
  dk_deep_link: string | null;
  dk_event_id: string | null;
  dk_outcome_code: string | null;
  fd_price: number | null;
  fd_deep_link: string | null;
  fd_market_id: string | null;
  fd_selection_id: string | null;
  best_book: string | null;
  best_price: number | null;
  game_date_label: string;
  game_time_label: string;
};

export type ResearchPayload = {
  refreshed_at: string;
  categories: ResearchCategory[];
  teams: string[];
  positions: string[];
  games: ResearchGame[];
  props: ResearchProp[];
};

type UseResearchResult = {
  data: ResearchPayload | null;
  loading: boolean;
  error: string | null;
  refreshedAt: string | null;
  cacheSource: string | null;
  refetch: () => void;
};

export function useNbaResearch(): UseResearchResult {
  const [data, setData] = useState<ResearchPayload | null>(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [refreshedAt, setRefreshedAt] = useState<string | null>(null);
  const [cacheSource, setCacheSource] = useState<string | null>(null);
  const [tick, setTick] = useState(0);

  const refetch = useCallback(() => setTick((t) => t + 1), []);

  useEffect(() => {
    let cancelled = false;
    setLoading(true);
    setError(null);

    const url = `${API_BASE}nba/research`;
    fetch(url)
      .then(async (res) => {
        if (!res.ok) {
          throw new Error(`HTTP ${res.status}`);
        }
        const headerSource = res.headers.get("x-pulse-cache-source");
        const headerRefreshed = res.headers.get("x-pulse-cache-refreshed-at");
        const payload = (await res.json()) as ResearchPayload;
        if (cancelled) return;
        setData(payload);
        setRefreshedAt(headerRefreshed ?? payload.refreshed_at ?? null);
        setCacheSource(headerSource);
      })
      .catch((e) => {
        if (cancelled) return;
        setError(e instanceof Error ? e.message : String(e));
      })
      .finally(() => {
        if (!cancelled) setLoading(false);
      });

    return () => {
      cancelled = true;
    };
  }, [tick]);

  return { data, loading, error, refreshedAt, cacheSource, refetch };
}
