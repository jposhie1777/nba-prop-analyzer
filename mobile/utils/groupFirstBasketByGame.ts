import { FirstBasketRow } from "@/hooks/useFirstBasket";

export function groupFirstBasketByGame(rows: FirstBasketRow[]) {
  const games = new Map<number, {
    game_id: number;
    game_date: string;
    teams: Map<string, number>;
    players: FirstBasketRow[];
  }>();

  for (const r of rows) {
    if (!games.has(r.game_id)) {
      games.set(r.game_id, {
        game_id: r.game_id,
        game_date: r.game_date,
        teams: new Map(),
        players: [],
      });
    }

    const g = games.get(r.game_id)!;

    g.teams.set(r.team_abbr, r.team_tip_win_pct);
    g.players.push(r);
  }

  return Array.from(games.values()).map(g => ({
    ...g,
    teams: Array.from(g.teams.entries()).map(([abbr, pct]) => ({
      team_abbr: abbr,
      tip_win_pct: pct,
    })),
    players: g.players.sort(
      (a, b) => a.rank_within_game - b.rank_within_game
    ),
  }));
}