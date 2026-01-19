// components/live/playerpropcard
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
    <View style={styles.wrapper}>
      {/* LEFT ACCENT RAIL */}
      <View
        style={[
          styles.accent,
          { backgroundColor: colors.accent.primary },
        ]}
      />

      {/* CARD */}
      <View
        style={[
          styles.card,
          {
            backgroundColor: colors.surface.card,
            borderColor: colors.border.subtle,
          },
        ]}
      >
        {/* HEADER */}
        <View style={styles.header}>
          <Text
            style={[
              styles.name,
              { color: colors.text.primary },
            ]}
          >
            {name}
          </Text>

          <Text style={{ color: colors.text.muted, fontSize: 12 }}>
            {minutes} min · PTS {current.pts} · REB {current.reb} · AST {current.ast}
          </Text>
        </View>

        {/* MARKETS */}
        {Object.entries(player.markets).map(
          ([market, marketData]: any) => {
            const currentByMarket: Record<string, number> = {
              pts: current.pts,
              ast: current.ast,
              reb: current.reb,
              "3pm": current.fg3m ?? 0,
            };
        
            return (
              <MarketRow
                key={`${player.player_id}-${market}`}
                playerId={player.player_id}
                market={market}
                lines={marketData.lines}
                current={currentByMarket[market] ?? 0}
                playerName={name}
              />
            );
          }
        )}
      </View>
    </View>
  );
}

const styles = StyleSheet.create({
  wrapper: {
    flexDirection: "row",
    marginBottom: 14,
  },

  accent: {
    width: 4,
    borderRadius: 4,
    marginRight: 10,
  },

  card: {
    flex: 1,
    borderWidth: 1,
    borderRadius: 16,
    padding: 14,
    gap: 10,

    // iOS elevation
    shadowColor: "#000",
    shadowOpacity: 0.06,
    shadowRadius: 10,
    shadowOffset: { width: 0, height: 4 },

    // Android elevation
    elevation: 3,
  },

  header: {
    gap: 2,
  },

  name: {
    fontSize: 15,
    fontWeight: "800",
  },
});