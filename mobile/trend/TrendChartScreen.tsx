import { View, Text, StyleSheet } from "react-native";
import { useMemo, useState } from "react";
import { useTheme } from "@/store/useTheme";
import { useHistoricalPlayerTrends } from "@/hooks/useHistoricalPlayerTrends";
import { resolveTrendSeries } from "./resolveTrendSeries";
import { TrendControls } from "./TrendControls";
import { TrendBarChart } from "./TrendBarChart";

export function TrendChartScreen({
  initialPlayer,
  initialMarket,
}: {
  initialPlayer?: string;
  initialMarket?: string;
}) {
  const colors = useTheme((s) => s.colors);
  const { ready, players, getByPlayer } = useHistoricalPlayerTrends();

  const [player, setPlayer] = useState<string | null>(
    initialPlayer ?? null
  );
  const [market, setMarket] = useState(
    initialMarket ?? "pts"
  );
  const [window, setWindow] = useState<5 | 10 | 20>(10);

  const trend = player ? getByPlayer(player) : undefined;

  const { values, dates } = useMemo(
    () => resolveTrendSeries(trend, market, window),
    [trend, market, window]
  );

  if (!ready) {
    return (
      <View style={styles.center}>
        <Text style={{ color: colors.text.muted }}>
          Loading trends…
        </Text>
      </View>
    );
  }

  return (
    <View style={styles.screen}>
      <TrendControls
        players={players}
        player={player}
        market={market}
        window={window}
        onPlayer={setPlayer}
        onMarket={setMarket}
        onWindow={setWindow}
      />

      <View
        style={[
          styles.chartCard,
          {
            backgroundColor: colors.surface.cardSoft,
            borderColor: colors.border.subtle,
          },
        ]}
      >
        {/* CHART TITLE / CONTEXT */}
        <Text
          style={{
            fontSize: 13,
            fontWeight: "800",
            letterSpacing: 0.4,
            color: colors.text.muted,
            textAlign: "center",
            marginBottom: 8,
          }}
        >
          {market.toUpperCase()} · LAST {window}
        </Text>
      
        {/* SPACING ANCHOR */}
        <View style={{ height: 8 }} />
      
        {/* CHART */}
        {values.length ? (
          <TrendBarChart values={values} dates={dates} />
        ) : (
          <Text
            style={{
              color: colors.text.muted,
              textAlign: "center",
              paddingVertical: 24,
            }}
          >
            Select a player to view trends
          </Text>
        )}
      </View>
    </View>
  );
}

const styles = StyleSheet.create({
  screen: {
    flex: 1,
    padding: 14,
  },
  chartCard: {
    marginTop: 14,
    borderRadius: 18,
    paddingVertical: 12,
  },
  center: {
    flex: 1,
    justifyContent: "center",
    alignItems: "center",
  },
  chartHeaderSpacer: {
    height: 12,
  },
});