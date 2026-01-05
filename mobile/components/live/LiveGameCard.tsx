import { View, StyleSheet } from "react-native";
import { useTheme } from "@/store/useTheme";
import { LiveGame } from "@/types/live";

import { GameHeader } from "./GameHeader";
import { ScoreRow } from "./ScoreRow";
import { GameStatus } from "./GameStatus";
import { BoxScoreStub } from "./BoxScoreStub";
import { LiveOdds } from "./LiveOdds";

export function LiveGameCard({ game }: { game: LiveGame }) {
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

      <BoxScoreStub />

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
