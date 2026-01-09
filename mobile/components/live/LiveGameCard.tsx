import { View, StyleSheet } from "react-native";
import { useTheme } from "@/store/useTheme";
import { LiveGame } from "@/types/live";
import { LivePlayerStat } from "@/hooks/useLivePlayerStats";

import { GameHeader } from "./GameHeader";
import { ScoreRow } from "./ScoreRow";
import { GameStatus } from "./GameStatus";
import { BoxScore } from "./boxscore/BoxScore";
import { LiveOdds } from "./LiveOdds";

type Props = {
  game: LiveGame;
  players: LivePlayerStat[];
};

export function LiveGameCard({ game, players }: Props) {
  const { colors } = useTheme();

  const hasOdds =
    !!game.odds?.spread?.length ||
    !!game.odds?.total?.length ||
    !!game.odds?.moneyline?.length;

  return (
    <View style={[styles.card, { backgroundColor: colors.surface.card }]}>
      <GameHeader home={game.home} away={game.away} />
      <ScoreRow game={game} />
      <GameStatus game={game} />

      <View
        style={[
          styles.divider,
          { backgroundColor: colors.border.subtle },
        ]}
      />

      <BoxScore
        homeTeam={game.home.abbreviation}
        awayTeam={game.away.abbreviation}
        players={players}
      />

      {hasOdds && (
        <>
          <View
            style={[
              styles.divider,
              { backgroundColor: colors.border.subtle },
            ]}
          />
          <LiveOdds
            odds={game.odds!}
            home={game.home}
            away={game.away}
          />
        </>
      )}
    </View>
  );
}