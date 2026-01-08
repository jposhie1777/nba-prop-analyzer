import { View, Text, StyleSheet, Pressable } from "react-native";
import { useMemo, useState } from "react";
import { useTheme } from "@/store/useTheme";
import { useHistoricalPlayerTrends } from "@/hooks/useHistoricalPlayerTrends";
import { resolveSparklineByMarket } from "@/utils/resolveSparkline";
import { Sparkline } from "@/components/Sparkline";
import { PlayerDropdown } from "@/components/PlayerDropdown";

export default function TrendChartScreen() {
  const colors = useTheme((s) => s.colors);
  const styles = useMemo(() => makeStyles(colors), [colors]);

  // ✅ SINGLE HOOK CALL
  const { ready, players, getByPlayer } = useHistoricalPlayerTrends();

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
        <PlayerDropdown
          players={players}
          value={player}
          onSelect={setPlayer}
        />

        <Pressable style={styles.select}>
          <Text style={styles.selectLabel}>{market}</Text>
        </Pressable>

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

const makeStyles = (colors: any) =>
  StyleSheet.create({
    root: { flex: 1 },
    screen: {
      flex: 1,
      backgroundColor: colors.surface.screen,
      padding: 14,
    },

    title: {
      fontSize: 18,
      fontWeight: "900",
      color: colors.text.primary,
      marginBottom: 10,
    },

    section: {
      marginTop: 12,
      gap: 8,
    },

    label: {
      fontSize: 12,
      fontWeight: "800",
      color: colors.text.muted,
      letterSpacing: 0.3,
    },

    card: {
      backgroundColor: colors.surface.card,
      borderRadius: 16,
      padding: 12,
      borderWidth: 1,
      borderColor: colors.border.subtle,
    },

    row: {
      flexDirection: "row",
      gap: 10,
      alignItems: "center",
    },

    pillGroup: {
      flexDirection: "row",
      backgroundColor: colors.surface.cardSoft,
      borderRadius: 999,
      padding: 4,
      gap: 6,
      alignSelf: "flex-start",
    },

    pill: {
      paddingHorizontal: 14,
      paddingVertical: 6,
      borderRadius: 999,
      backgroundColor: "transparent",
    },

    pillActive: {
      backgroundColor: colors.surface.card,
      borderWidth: 1,
      borderColor: colors.border.subtle,
    },

    pillLabel: {
      fontSize: 12,
      fontWeight: "800",
      color: colors.text.muted,
    },
    center: {
      flex: 1,
      justifyContent: "center",
      alignItems: "center",
    },

    muted: {
      color: colors.text.muted,
      fontWeight: "600",
    },

    controls: {
      gap: 12,
      marginBottom: 14,
    },

    select: {
      backgroundColor: colors.surface.card,
      paddingHorizontal: 12,
      paddingVertical: 10,
      borderRadius: 12,
      borderWidth: 1,
      borderColor: colors.border.subtle,
    },

    selectLabel: {
      fontWeight: "800",
      color: colors.text.primary,
    },

    toggleGroup: {
      flexDirection: "row",
      backgroundColor: colors.surface.cardSoft,
      borderRadius: 999,
      padding: 4,
      gap: 6,
      alignSelf: "flex-start",
    },

    toggle: {
      paddingHorizontal: 14,
      paddingVertical: 6,
      borderRadius: 999,
    },

    toggleActive: {
      backgroundColor: colors.surface.card,
      borderWidth: 1,
      borderColor: colors.border.subtle,
    },

    toggleLabel: {
      fontSize: 12,
      fontWeight: "800",
      color: colors.text.muted,
    },

    toggleLabelActive: {
      color: colors.text.primary,
    },

    chartCard: {
      backgroundColor: colors.surface.card,
      borderRadius: 18,
      padding: 14,
      borderWidth: 1,
      borderColor: colors.border.subtle,
    },


    pillLabelActive: {
      color: colors.text.primary,
    },
  });
