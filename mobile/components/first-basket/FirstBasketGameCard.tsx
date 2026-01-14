import { View, Text } from "react-native";
import { useTheme } from "@/store/useTheme";

export function FirstBasketGameCard({ game }: any) {
  const { colors } = useTheme();

  const teams = Array.isArray(game.teams) ? game.teams : [];
  const away = teams[0];
  const home = teams[1];

  const players = Array.isArray(game.players) ? game.players : [];

  return (
    <View
      style={{
        backgroundColor: colors.surface.card,
        margin: 12,
        padding: 12,
        borderRadius: 12,
      }}
    >
      {/* Matchup */}
      <Text style={{ fontWeight: "600" }}>
        {away?.team_abbr ?? "—"} @ {home?.team_abbr ?? "—"}
      </Text>

      {/* Tip win (only if available) */}
      {away && home && (
        <Text style={{ color: colors.text.muted, marginTop: 4 }}>
          Tip Win: {away.team_abbr}{" "}
          {Math.round((away.tip_win_pct ?? 0) * 100)}% ·{" "}
          {home.team_abbr}{" "}
          {Math.round((home.tip_win_pct ?? 0) * 100)}%
        </Text>
      )}

      {/* Players */}
      {players.slice(0, 5).map((p: any) => (
        <View
          key={p.player_id}
          style={{
            flexDirection: "row",
            justifyContent: "space-between",
            marginTop: 6,
          }}
        >
          <Text>
            {p.rank_within_game}. {p.player}
          </Text>
          <Text>
            {((p.first_basket_probability ?? 0) * 100).toFixed(1)}%
          </Text>
        </View>
      ))}
    </View>
  );
}