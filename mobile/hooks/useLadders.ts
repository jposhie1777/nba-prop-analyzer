// hooks/useLadders.ts
import { useState, useEffect } from "react";

export type VendorBlock = {
  vendor: string;
  line: number;
  over_odds: number | null;
  under_odds: number | null;
};

export type Ladder = {
  game_id: number;
  player_id: number;
  player_name: string;
  player_team_abbr: string;
  opponent_team_abbr: string;
  market: string;
  ladder_tier: string;
  anchor_line: number;
  ladder_score: number;
  ladder_by_vendor: VendorBlock[];
};

export function useLadders() {
  const [data, setData] = useState<Ladder[]>([]);
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    // TODO: Replace with actual API call when /ladders endpoint is available
    // For now, return empty data after a brief delay to simulate loading
    const timer = setTimeout(() => {
      setData([]);
      setLoading(false);
    }, 500);

    return () => clearTimeout(timer);
  }, []);

  return { data, loading };
}
