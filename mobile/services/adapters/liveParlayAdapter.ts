import { LivePlayerStat } from "@/hooks/useLivePlayerStats";
import { LiveGame } from "@/types/live";
import { LiveSnapshotByPlayerId } from "@/store/useParlayTracker";

/**
 * Build a live snapshot keyed by player_id
 * - Games are the source of truth for clock / period / final
 * - Players supply stats
 */
export function buildLiveSnapshot({
  players,
  games,
}: {
  players: LivePlayerStat[];
  games: LiveGame[];
}): LiveSnapshotByPlayerId {
  const gameContext = new Map<number, {
    game_status: "pregame" | "live" | "final";
    period: number | null;
    clock: string | null;
  }>();

  /* ======================================================
     1️⃣ INDEX GAMES (AUTHORITATIVE)
  ====================================================== */
  for (const g of games ?? []) {
    const gameId = Number(g.game_id ?? g.id);
    if (!gameId) continue;

    const rawStatus = String(
      g.game_status ?? g.status ?? ""
    ).toLowerCase();

    const game_status =
      rawStatus.includes("final")
        ? "final"
        : rawStatus.includes("live") || rawStatus.includes("progress")
        ? "live"
        : "pregame";

    gameContext.set(gameId, {
      game_status,
      period: g.period ?? g.quarter ?? null,
      clock: g.clock ?? g.game_clock ?? null,
    });
  }

  /* ======================================================
     2️⃣ MERGE PLAYERS + GAME CONTEXT
  ====================================================== */
  const snapshot: LiveSnapshotByPlayerId = {};

  for (const p of players ?? []) {
    const playerId = Number(p.player_id ?? p.id);
    if (!playerId) continue;

    const gameId = Number(p.game_id);
    const ctx = gameContext.get(gameId);

    snapshot[playerId] = {
      /* ---------- Stats ---------- */
      pts: p.pts ?? 0,
      reb: p.reb ?? 0,
      ast: p.ast ?? 0,
      fg3m: p.fg3m ?? 0,

      /* ---------- Game Context ---------- */
      game_id: gameId,
      game_status: ctx?.game_status ?? "live",
      period: ctx?.period ?? null,
      clock: ctx?.clock ?? null,
    };
  }

  return snapshot;
}