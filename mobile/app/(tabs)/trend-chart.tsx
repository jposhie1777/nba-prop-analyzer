import { View, Text, StyleSheet, Pressable } from "react-native";
import { useMemo, useState } from "react";
import { useTheme } from "@/store/useTheme";
import { useHistoricalPlayerTrends } from "@/hooks/useHistoricalPlayerTrends";
import { resolveSparklineByMarket } from "@/utils/resolveSparkline";
import { Sparkline } from "@/components/Sparkline";

/* ======================================================
   SCREEN
====================================================== */
export default function TrendChartScreen() {
  const colors = useTheme((s) => s.colors);
  const styles = useMemo(() => makeStyles(colors), [colors]);

  const { ready, getByPlayer } = useHistoricalPlayerTrends();

  /* ---------------------------
     LOCAL STATE
  --------------------------- */
  const [player, setPlayer] = useState<string | null>(null);
  const [market, setMarket] = useState<string>("PTS");
  const [window, setWindow] = useState<5 | 10 | 20>(10);

  /* ---------------------------
     RESOLVE DATA
  --------------------------- */
  const trend = player ? getByPlayer(player) : undefined;
  const spark = resolveSparklineByMarket(market, trend);

  const data =
    window === 5
      ? spark.sparkline_l5
      : window === 20
      ? spark.sparkline_l20
      : spark.sparkline_l10;

  /* ---------------------------
     EMPTY STATES
  --------------------------- */
  if (!ready) {
    return (
      <View style={styles.center}>
        <Text style={styles.muted}>Loading trends…</Text>
      </View>
    );
  }

  return (
    <View style={styles.screen}>
      {/* =========================
          CONTROLS
      ========================== */}
      <View style={styles.controls}>
        {/* PLAYER SELECT (stub for now) */}
        <Pressable style={styles.select}>
          <Text style={styles.selectLabel}>
            {player ?? "Select Player"}
          </Text>
        </Pressable>

        {/* MARKET SELECT */}
        <Pressable style={styles.select}>
          <Text style={styles.selectLabel}>{market}</Text>
        </Pressable>

        {/* WINDOW TOGGLE */}
        <View style={styles.toggleGroup}>
          {[5, 10, 20].map((n) => {
            const active = window === n;
            return (
              <Pressable
                key={n}
                onPress={() => setWindow(n as 5 | 10 | 20)}
                style={[
                  styles.toggle,
                  active && styles.toggleActive,
                ]}
              >
                <Text
                  style={[
                    styles.toggleLabel,
                    active && styles.toggleLabelActive,
                  ]}
                >
                  L{n}
                </Text>
              </Pressable>
            );
          })}
        </View>
      </View>

      {/* =========================
          CHART
      ========================== */}
      <View style={styles.chartCard}>
        {data && data.length > 0 ? (
          <Sparkline data={data} height={120} />
        ) : (
          <Text style={styles.muted}>
            Select a player to view trends
          </Text>
        )}
      </View>
    </View>
  );
}