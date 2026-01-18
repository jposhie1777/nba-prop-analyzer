// mobile/hooks/useFirstBasketMatchups.ts
import { useEffect, useState } from "react";
import Constants from "expo-constants";

const API_URL =
  process.env.EXPO_PUBLIC_API_URL ||
  Constants.expoConfig?.extra?.EXPO_PUBLIC_API_URL ||
  "";


export type FirstBasketSide = {
  player: string;
  firstBasketPct: number;
  firstShotShare: number;
  playerFirstBasketCount: number;
  playerTeamFirstBasketCount: number;
};

export type FirstBasketRow = {
  rank: number;
  home: FirstBasketSide | null;
  away: FirstBasketSide | null;
};

export type FirstBasketMatchup = {
  gameId: number;
  homeTeam: string;
  awayTeam: string;
  rows: FirstBasketRow[];

  homeTipWinPct: number;
  awayTipWinPct: number;
};


export function useFirstBasketMatchups() {
  const [data, setData] = useState<FirstBasketMatchup[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);

  async function load() {
    try {
      setLoading(true);
      setError(null);

      const res = await fetch(`${API_URL}/first-basket/matchups`);
      if (!res.ok) throw new Error(`HTTP ${res.status}`);

      const json = await res.json();
      setData(json);
    } catch (e: any) {
      setError(e?.message ?? "Failed to load");
    } finally {
      setLoading(false);
    }
  }

  useEffect(() => {
    load();
  }, []);

  return { data, loading, error, refresh: load };
}
