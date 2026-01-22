import { View, Pressable, Text, StyleSheet, ScrollView } from "react-native";
import { PlayerDropdown } from "@/components/PlayerDropdown";
import { TREND_MARKETS } from "./TrendMarkets";
import type { TrendWindow } from "./resolveTrendSeries";
import { useTheme } from "@/store/useTheme";

type Props = {
  players: string[];
  player: string | null;
  market: string;
  window: TrendWindow;
  onPlayer: (p: string) => void;
  onMarket: (m: string) => void;
  onWindow: (w: TrendWindow) => void;
};

export function TrendControls({
  players,
  player,
  market,
  window,
  onPlayer,
  onMarket,
  onWindow,
}: Props) {
  const colors = useTheme((s) => s.colors);

  return (
    <View style={{ gap: 12 }}>
      <PlayerDropdown
        players={players}
        value={player}
        onSelect={onPlayer}
      />

      {/* MARKET SELECTOR */}
      <ScrollView
        horizontal
        showsHorizontalScrollIndicator={false}
        contentContainerStyle={styles.marketRow}
      >
        {TREND_MARKETS.map((m) => {
          const active = m.key === market;
      
          return (
            <Pressable
              key={m.key}
              onPress={() => onMarket(m.key)}
              style={[
                styles.marketPill,
                active && { backgroundColor: colors.state.selected },
              ]}
            >
              <Text
                style={[
                  styles.marketLabel,
                  active && { color: colors.state.active },
                ]}
              >
                {m.label}
              </Text>
            </Pressable>
          );
        })}
      </ScrollView>

      <View style={styles.windowRow}>
        {[5, 10, 20].map((n) => {
          const active = window === n;
          return (
            <Pressable
              key={n}
              onPress={() => onWindow(n as TrendWindow)}
              style={[
                styles.windowPill,
                active && { backgroundColor: colors.surface.card },
              ]}
            >
              <Text
                style={[
                  styles.windowLabel,
                  active && { color: colors.text.primary },
                ]}
              >
                L{n}
              </Text>
            </Pressable>
          );
        })}
      </View>
    </View>
  );
}

const styles = StyleSheet.create({
  marketRow: {
    flexDirection: "row",
    flexWrap: "wrap",
    gap: 6,
  },
  marketPill: {
    paddingHorizontal: 12,
    paddingVertical: 6,
    borderRadius: 999,
  },
  marketLabel: {
    fontSize: 12,
    fontWeight: "800",
    color: "#888",
  },
  windowRow: {
    flexDirection: "row",
    gap: 6,
    alignSelf: "flex-start",
  },
  windowPill: {
    paddingHorizontal: 14,
    paddingVertical: 6,
    borderRadius: 999,
  },
  windowLabel: {
    fontSize: 12,
    fontWeight: "800",
    color: "#888",
  },
});