import { View, Text } from "react-native";
import { useTheme } from "@/store/useTheme";

export function FirstBasketGameCard({ game }: any) {
  const { colors } = useTheme();
  const [away, home] = game.teams;

  return (
    <View style={{
      backgroundColor: colors.surface.card,
      margin: 12,
      padding: 12,
      borderRadius: 12
    }}>
      <Text style={{ fontWeight: "600" }}>
        {away.team_abbr} @ {home.team_abbr}
      </Text>

      <Text style={{ color: colors.text.muted, marginTop: 4 }}>
        Tip Win: {away.team_abbr} {Math.round(away.tip_win_pct * 100)}% ·
        {home.team_abbr} {Math.round(home.tip_win_pct * 100)}%
      </Text>

      {game.players.slice(0, 5).map(p => (
        <View
          key={p.player_id}
          style={{ flexDirection: "row", justifyContent: "space-between" }}
        >
          <Text>{p.rank_within_game}. {p.player}</Text>
          <Text>
            {(p.first_basket_probability * 100).toFixed(1)}%
          </Text>
        </View>
      ))}
    </View>
  );
}