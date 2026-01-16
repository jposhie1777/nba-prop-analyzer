import { View, Text } from "react-native";
import { useTheme } from "@/store/useTheme";
import { TeamLineups } from "./TeamLineups";

export function GameLineupCard({ game }: { game: any }) {
  const { colors } = useTheme();

  return (
    <View
      style={{
        backgroundColor: colors.surface.card
        borderRadius: 12,
        padding: 12,
        gap: 12,
      }
    >
      {/* Header */}
      <View>
        <Text style={{ color: colors.text.primary, fontSize: 16, fontWeight: "600" }}>
          {game.away_team_abbr} @ {game.home_team_abbr}
        </Text>
        <Text style={{ color: colors.text.muted, fontSize: 12 }}>
          {game.state}
        </Text>
      </View>

      {/* Away */}
      <TeamLineups
        teamAbbr={game.away_team_abbr}
        mostCommon={game.most_common_lineups}
        projected={game.projected_lineups}
      />

      {/* Home */}
      <TeamLineups
        teamAbbr={game.home_team_abbr}
        mostCommon={game.most_common_lineups}
        projected={game.projected_lineups}
      />
    </View>
  );
}