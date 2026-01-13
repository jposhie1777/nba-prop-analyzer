import { View, Text, StyleSheet } from "react-native";
import { useTheme } from "@/store/useTheme";
import { MarketRow } from "./MarketRow";

export function PlayerPropCard({
  player,
  name,
  minutes,
  current,
}: any) {
  const { colors } = useTheme();

  return (
    <View
      style={[
        styles.card,
        { borderColor: colors.border.subtle },
      ]}
    >
      {/* HEADER */}
      <View style={styles.header}>
        <Text style={styles.name}>{name}</Text>
        <Text style={{ color: colors.text.muted }}>
          {minutes} min · PTS {current.pts} · REB {current.reb} · AST {current.ast}
        </Text>
      </View>

      {/* MARKETS */}
      {Object.entries(player.markets).map(
        ([market, marketData]: any) => (
          <MarketRow
            key={market}
            market={market}
            lines={marketData.lines}
          />
        )
      )}
    </View>
  );
}

const styles = StyleSheet.create({
  card: {
    borderWidth: 1,
    borderRadius: 14,
    padding: 12,
    gap: 10,
  },
  header: {
    gap: 2,
  },
  name: {
    fontSize: 14,
    fontWeight: "800",
  },
});