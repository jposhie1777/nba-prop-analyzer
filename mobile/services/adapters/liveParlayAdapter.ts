import { LivePlayerStat } from "@/hooks/useLivePlayerStats";
import { LiveGame } from "@/types/live";

export function buildLiveSnapshot({
  players,
  games,
}: {
  players: any[];
  games: any[];
}) {
  const snapshot: Record<number, any> = {};

  for (const p of players) {
    if (!p.player_id) continue;

    snapshot[Number(p.player_id)] = {
      pts: p.pts,
      reb: p.reb,
      ast: p.ast,
      fg3m: p.fg3m,

      game_id: p.game_id,
      period: p.period ?? null,
      clock: p.clock ?? null,
      game_status: p.game_status ?? "live",
    };
  }

  return snapshot;
}

