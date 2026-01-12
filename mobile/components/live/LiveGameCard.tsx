// components/live/LiveGameCard.tsx
import { View, Text, StyleSheet } from "react-native";
import { useMemo } from "react";

import { useTheme } from "@/store/useTheme";
import { useLiveStore } from "@/store/liveStore";

import { GameHeader } from "./GameHeader";
import { ScoreRow } from "./ScoreRow";
import { GameStatus } from "./GameStatus";
import { BoxScore } from "./boxscore/BoxScore";
import { LiveOdds } from "./LiveOdds";

type Props = {
  game: any;              // already adapted LiveGame
  players: any[];         // LivePlayerStat[]
};

export function LiveGameCard({ game, players }: Props) {
  const { colors } = useTheme();

  // ------------------------------------
  // 🧠 GAME ODDS (from store)
  // ------------------------------------
  const gameOdds = useLiveStore(
    (s) => s.oddsByGameId[game.gameId]
  );

  // ------------------------------------
  // 🧠 PLAYER PROP MARKETS (from store)
  // ------------------------------------
  const playerIds = useLiveStore(
    (s) => s.playerIdsByGame[game.gameId]
  );

  const propMarketsByKey = useLiveStore(
    (s) => s.propMarketsByKey
  );

  // ------------------------------------
  // Player metadata lookup
  // ------------------------------------
  const playerMetaById = useMemo(() => {
    const map = new Map<number, any>();
    for (const p of players) {
      map.set(p.player_id, p);
    }
    return map;
  }, [players]);

  // ------------------------------------
  // Build grouped props for UI
  // ------------------------------------
  const groupedProps = useMemo(() => {
    if (!playerIds) return [];

    const out: any[] = [];

    for (const playerId of playerIds) {
      const meta = playerMetaById.get(playerId);
      if (!meta) continue;

      const keyPrefix = `${game.gameId}:${playerId}:`;

      const markets = Object.entries(propMarketsByKey)
        .filter(([k]) => k.startsWith(keyPrefix))
        .map(([, v]) => v);

      if (markets.length === 0) continue;

      out.push({
        player_id: playerId,
        name: meta.name,
        team: meta.team,
        markets,
      });
    }

    return out;
  }, [playerIds, propMarketsByKey, playerMetaById, game.gameId]);

  return (
    <View style={[styles.card, { backgroundColor: colors.surface.card }]}>
      <GameHeader home={game.home} away={game.away} />
      <ScoreRow game={game} />
      <GameStatus game={game} />

      <View style={[styles.divider, { backgroundColor: colors.border.subtle }]} />

      <BoxScore
        homeTeam={game.home.team}
        awayTeam={game.away.team}
        players={players}
      />

      {/* 🟢 GAME ODDS (spread / total) */}
      {gameOdds && (
        <View style={{ marginBottom: 8 }}>
          {gameOdds.spread?.map((s) => (
            <Text
              key={s.selectionId}
              style={{ color: colors.text.muted, fontSize: 12 }}
            >
              {s.outcome} {s.line} ({s.best.odds})
            </Text>
          ))}
        </View>
      )}

      <View style={[styles.divider, { backgroundColor: colors.border.subtle }]} />

      <LiveOdds groupedProps={groupedProps} />
    </View>
  );
}