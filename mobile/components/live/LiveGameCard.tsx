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
import { useLiveGameOdds } from "@/hooks/useLiveGameOdds";
import { useLivePlayerProps } from "@/hooks/useLivePlayerProps";

type Props = {
  game: any;      // adapted LiveGame
  players: any[]; // LivePlayerStat[]
};

export function LiveGameCard({ game, players }: Props) {
  const { colors } = useTheme();

  // 🔥 PER-GAME ODDS POLLING
  useLiveGameOdds(Number(game.gameId));
  useLivePlayerProps(Number(game.gameId));

  if (__DEV__) {
    const propKeysForGame = Object.keys(propMarketsByKey || {}).filter(
      (k) => k.startsWith(`${game.gameId}:`)
    );
  
    console.log("🧪 PROP KEYS FOR GAME", {
      gameId: game.gameId,
      count: propKeysForGame.length,
      sample: propKeysForGame.slice(0, 3),
    });
  }
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
  // Build player blocks for LiveOdds
  // ------------------------------------
  const playersForOdds = useMemo(() => {
    const out: any[] = [];
  
    // ------------------------------------
    // Group prop markets by propPlayerId
    // ------------------------------------
    const marketsByPropPlayerId: Record<string, any[]> = {};
  
    for (const [key, market] of Object.entries(propMarketsByKey)) {
      if (!key.startsWith(`${game.gameId}:`)) continue;
  
      // key format: gameId:playerId:marketKey
      const [, propPlayerId] = key.split(":");
  
      if (!marketsByPropPlayerId[propPlayerId]) {
        marketsByPropPlayerId[propPlayerId] = [];
      }
  
      marketsByPropPlayerId[propPlayerId].push(market);
    }
  
    // ------------------------------------
    // 🧪 DEBUG A — Do props exist for this game?
    // ------------------------------------
    if (__DEV__) {
      console.log("🧪 DEBUG A — marketsByPropPlayerId", {
        gameId: game.gameId,
        propPlayerCount: Object.keys(marketsByPropPlayerId).length,
        sampleKeys: Object.keys(marketsByPropPlayerId).slice(0, 3),
      });
    }
  
    // ------------------------------------
    // 🧪 DEBUG B — Do prop markets have player names?
    // ------------------------------------
    if (__DEV__) {
      const samples = Object.values(marketsByPropPlayerId)
        .slice(0, 3)
        .map((markets: any[]) => ({
          playerName: markets[0]?.playerName,
          marketKey: markets[0]?.marketKey,
        }));
  
      console.log("🧪 DEBUG B — prop name samples", samples);
    }
  
    // ------------------------------------
    // Match prop players → boxscore players BY NAME
    // ------------------------------------
    for (const markets of Object.values(marketsByPropPlayerId)) {
      const sampleMarket = markets[0];
      const rowPlayerName = sampleMarket?.playerName;
  
      if (!rowPlayerName) continue;
  
      const meta = players.find((p) => {
        if (!p.name) return false;
  
        const match =
          p.name.toLowerCase() === rowPlayerName.toLowerCase();
  
        // ------------------------------------
        // 🧪 DEBUG C — Name match confirmation
        // ------------------------------------
        if (__DEV__ && match) {
          console.log("🟢 DEBUG C — NAME MATCH", {
            propName: rowPlayerName,
            statName: p.name,
            player_id: p.player_id,
          });
        }
  
        return match;
      });
  
      if (!meta) continue;
  
      out.push({
        player_id: meta.player_id,
        name: meta.name,
        team: meta.team,
        markets,
      });
    }
  
    // ------------------------------------
    // 🧪 FINAL — What actually made it through?
    // ------------------------------------
    if (__DEV__) {
      console.log("🧪 FINAL — playersForOdds result", {
        gameId: game.gameId,
        matchedPlayers: out.length,
        names: out.map((p) => p.name),
      });
    }
  
    return out;
  }, [players, propMarketsByKey, game.gameId]);

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

      {/* 🟢 PLAYER PROPS (NEW MODEL) */}
      <LiveOdds players={playersForOdds} />
    </View>
  );
}

const styles = StyleSheet.create({
  card: {
    borderRadius: 16,
    marginHorizontal: 12,
    marginTop: 12,
    padding: 12,
  },
  divider: {
    height: 1,
    marginVertical: 8,
  },
});