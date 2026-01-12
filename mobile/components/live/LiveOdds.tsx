// components/live/LiveOdds.tsx
import { View, Text, StyleSheet } from "react-native";
import { useTheme } from "@/store/useTheme";

type Props = {
  groupedProps: Record<number, any>;
  loading: boolean;
  home: any;
  away: any;

  // 🔴 ADD THIS
  playerNameById: Map<number, string>;
};

export function LiveOdds({
  groupedProps,
  loading,
  playerNameById,
}: Props) {
  const { colors } = useTheme();

  if (loading) {
    return (
      <Text style={{ color: colors.text.muted, marginTop: 8 }}>
        Loading live props…
      </Text>
    );
  }

  const players = Object.values(groupedProps);

  if (players.length === 0) {
    return (
      <Text style={{ color: colors.text.muted, marginTop: 8 }}>
        No live props available
      </Text>
    );
  }

  return (
    <View style={styles.wrap}>
      {players.map((player: any) => (
        <View
          key={player.player_id}
          style={[
            styles.playerBlock,
            { borderColor: colors.border.subtle },
          ]}
        >
          <Text
            style={[
              styles.playerLabel,
              { color: colors.text.primary },
            ]}
          >
            {playerNameById.get(player.player_id) ?? `Player ${player.player_id}`}
          </Text>

          {Object.entries(player.markets).map(
            ([market, marketData]: any) => (
              <View key={market} style={styles.marketRow}>
                <Text
                  style={[
                    styles.marketLabel,
                    { color: colors.text.secondary },
                  ]}
                >
                  {market} — Line {marketData.line}
                </Text>

                <View style={styles.bookRow}>
                  {Object.entries(marketData.books).map(
                    ([book, odds]: any) => (
                      <Text
                        key={book}
                        style={{ color: colors.text.muted }}
                      >
                        {book.toUpperCase()}: O {odds.over} / U{" "}
                        {odds.under}
                      </Text>
                    )
                  )}
                </View>
              </View>
            )
          )}
        </View>
      ))}
    </View>
  );
}

const styles = StyleSheet.create({
  wrap: {
    marginTop: 12,
    gap: 12,
  },

  playerBlock: {
    borderWidth: 1,
    borderRadius: 12,
    padding: 10,
    gap: 6,
  },

  playerLabel: {
    fontSize: 13,
    fontWeight: "800",
  },

  marketRow: {
    gap: 2,
  },

  marketLabel: {
    fontSize: 12,
    fontWeight: "700",
  },

  bookRow: {
    flexDirection: "row",
    gap: 12,
  },
});