// components/live/liveGameCard
import { View, Text, StyleSheet } from "react-native";
import { useTheme } from "@/store/useTheme";
import { LiveGame } from "@/types/live";
import { LivePlayerStat } from "@/hooks/useLivePlayerStats";

import { GameHeader } from "./GameHeader";
import { ScoreRow } from "./ScoreRow";
import { GameStatus } from "./GameStatus";
import { BoxScore } from "./boxscore/BoxScore";
import { LiveOdds } from "./LiveOdds";
import { useLivePlayerProps } from "@/hooks/useLivePlayerProps";
import { groupLiveProps } from "@/utils/groupLiveProps";
import { useMemo } from "react";
import { useLiveGameOdds } from "@/hooks/useLiveGameOdds";

type Props = {
  game: LiveGame;
  players: LivePlayerStat[];
};

export function LiveGameCard({ game, players }: Props) {
  const { colors } = useTheme();

  const {
    props: liveProps,
    loading: oddsLoading,
  } = useLivePlayerProps(game.gameId);

  console.log("🧪 Live props raw", {
    gameId: game.gameId,    // ✅ CORRECT
    count: liveProps.length,
    sample: liveProps[0],
  });

  const groupedLiveProps = useMemo(
    () => groupLiveProps(liveProps),
    [liveProps]
  );

  const {
    odds: gameOdds,
    loading: gameOddsLoading,
  } = useLiveGameOdds(game.gameId);
  
  console.log("🎯 GAME ODDS DEBUG", {
    gameId: game.gameId,
    count: gameOdds.length,
    sample: gameOdds[0],
  });

  const hasLiveOdds = Object.keys(groupedLiveProps).length > 0;

  return (
    <View style={[styles.card, { backgroundColor: colors.surface.card }]}>
      <GameHeader home={game.home} away={game.away} />
      <ScoreRow game={game} />
      <GameStatus game={game} />
      
      <View style={[styles.divider, { backgroundColor: colors.border.subtle }]} />
      
      {/* 🔴 DEBUG 5 */}
      <Text
        style={{
          fontSize: 10,
          color: colors.text.muted,
          textAlign: "center",
          marginBottom: 4,
        }}
      >
        DEBUG players for game {game.game_id}: {players.length}
      </Text>
      
      <BoxScore
        homeTeam={game.home.abbrev}
        awayTeam={game.away.abbrev}
        players={players}
      />
      
      {/* 🟢 LIVE GAME ODDS */}
      {gameOdds?.length > 0 && (
        <View style={{ marginBottom: 8 }}>
          {gameOdds.map((o) => (
            <Text
              key={o.book}
              style={{ color: colors.text.muted, fontSize: 12 }}
            >
              {o.book.toUpperCase()} —{" "}
              {game.away.abbrev} {o.spread_away} ({o.spread_away_odds}) ·{" "}
              {game.home.abbrev} {o.spread_home} ({o.spread_home_odds}) ·{" "}
              Total {o.total} O {o.over} / U {o.under}
            </Text>
          ))}
        </View>
      )}
      
      <View
        style={[
          styles.divider,
          { backgroundColor: colors.border.subtle },
        ]}
      />
      
      <LiveOdds
        groupedProps={groupedLiveProps}
        loading={oddsLoading}
        home={game.home}
        away={game.away}
      />
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