// mobile/app/(tabs)/live-props-dev.tsx
import { View, Text, FlatList, StyleSheet, Pressable } from "react-native";
import { useLivePropsDev } from "@/hooks/useLivePropsDev";
import { useTheme } from "@/store/useTheme";
import { useState } from "react";

/* ---------------------------------
   Pace helpers (kept as-is)
---------------------------------- */
function parseClock(clock?: string): number | null {
  if (!clock) return null;
  const [m, s] = clock.split(":").map(Number);
  if (Number.isNaN(m) || Number.isNaN(s)) return null;
  return m + s / 60;
}

function getElapsedMinutes(
  period?: string,
  clock?: string,
  gameState?: string
): number | null {
  if (gameState === "halftime") return 24;

  const quarterIndex: Record<string, number> = {
    Q1: 0,
    Q2: 1,
    Q3: 2,
    Q4: 3,
  };

  if (!period || !(period in quarterIndex)) return null;

  const remaining = parseClock(clock);
  if (remaining == null) return null;

  return quarterIndex[period] * 12 + (12 - remaining);
}

export default function LivePropsDevScreen() {
  const { colors } = useTheme();
  const { data, isLoading, error } = useLivePropsDev(50);

  if (isLoading) {
    return (
      <View style={[styles.center, { backgroundColor: colors.surface.screen }]}>
        <Text style={{ color: colors.text.muted }}>
          Loading live props…
        </Text>
      </View>
    );
  }

  if (error) {
    return (
      <View style={[styles.center, { backgroundColor: colors.surface.screen }]}>
        <Text style={{ color: colors.accent.danger }}>
          Error loading live props
        </Text>
      </View>
    );
  }

  if (!data || data.length === 0) {
    return (
      <View style={[styles.center, { backgroundColor: colors.surface.screen }]}>
        <Text style={{ color: colors.text.muted }}>
          No live props available
        </Text>
      </View>
    );
  }

  return (
    <View style={[styles.container, { backgroundColor: colors.surface.screen }]}>
      <FlatList
        data={data}
        keyExtractor={(item) =>
          `${item.game_id}-${item.player_id}-${item.market}-${item.line}-${item.book}`
        }
        contentContainerStyle={{ padding: 12 }}
        renderItem={({ item }) => {
          const [expanded, setExpanded] = useState(false);

          const gameContext =
            item.game_state === "halftime"
              ? "Halftime"
              : item.game_period && item.game_clock
              ? `${item.game_period} · ${item.game_clock}`
              : null;

          const elapsedMinutes = getElapsedMinutes(
            item.game_period,
            item.game_clock,
            item.game_state
          );

          const progress =
            elapsedMinutes && elapsedMinutes > 0
              ? elapsedMinutes / 48
              : null;

          const projectedFinal =
            progress && progress > 0.05
              ? item.current_stat / progress
              : null;

          return (
            <Pressable
              onPress={() => setExpanded((v) => !v)}
              style={[
                styles.card,
                {
                  backgroundColor: colors.surface.card,
                  borderColor: colors.border.subtle,
                },
              ]}
            >
              {/* =========================
                  COLLAPSED SECTION
              ========================== */}

              {/* Player */}
              <Text style={[styles.player, { color: colors.text.primary }]}>
                {item.player_name ?? "Unknown Player"}
              </Text>

              {/* Game context */}
              {gameContext && (
                <Text style={[styles.context, { color: colors.text.muted }]}>
                  {gameContext}
                </Text>
              )}

              {/* Market */}
              <Text style={[styles.title, { color: colors.text.primary }]}>
                {item.market.toUpperCase()} · {item.line}
              </Text>

              {/* Progress */}
              <Text style={[styles.body, { color: colors.text.secondary }]}>
                Current: {item.current_stat} → Need {item.remaining_needed}
              </Text>

              {/* Indicators */}
              <View style={styles.metricsRow}>
                {projectedFinal && (
                  <Text style={[styles.metric, { color: colors.text.secondary }]}>
                    Pace: {projectedFinal.toFixed(1)}
                  </Text>
                )}

                <Text
                  style={[
                    styles.metricStrong,
                    { color: colors.accent.warning },
                  ]}
                >
                  Blowout watch
                </Text>
              </View>

              {/* =========================
                  EXPANDED SECTION
              ========================== */}
              {expanded && (
                <View
                  style={[
                    styles.expanded,
                    { borderColor: colors.border.subtle },
                  ]}
                >
                  {/* Current */}
                  <Text
                    style={[
                      styles.expandedHeader,
                      { color: colors.text.primary },
                    ]}
                  >
                    Current
                  </Text>
                  <View style={styles.expandedRow}>
                    <Text style={[styles.cell, { color: colors.text.secondary }]}>
                      Projected: 28.4
                    </Text>
                    <Text style={[styles.cell, { color: colors.text.secondary }]}>
                      Minutes: 36.1
                    </Text>
                  </View>

                  {/* Historical H2 */}
                  <Text
                    style={[
                      styles.expandedHeader,
                      { color: colors.text.primary },
                    ]}
                  >
                    Historical H2
                  </Text>
                  <View style={styles.expandedRow}>
                    <Text style={[styles.cell, { color: colors.text.secondary }]}>
                      L5: 11.8
                    </Text>
                    <Text style={[styles.cell, { color: colors.text.secondary }]}>
                      L10: 10.9
                    </Text>
                    <Text style={[styles.cell, { color: colors.text.secondary }]}>
                      Min L5: 17.4
                    </Text>
                  </View>

                  {/* Risk Flags */}
                  <Text
                    style={[
                      styles.expandedHeader,
                      { color: colors.text.primary },
                    ]}
                  >
                    Risk Flags
                  </Text>
                  <Text
                    style={[
                      styles.risk,
                      { color: colors.accent.warning },
                    ]}
                  >
                    ⚠ Blowout Risk
                  </Text>
                </View>
              )}

              {/* Book */}
              <Text style={[styles.meta, { color: colors.text.muted }]}>
                Book: {item.book}
              </Text>
            </Pressable>
          );
        }}
      />
    </View>
  );
}

const styles = StyleSheet.create({
  container: { flex: 1 },

  center: {
    flex: 1,
    alignItems: "center",
    justifyContent: "center",
  },

  card: {
    padding: 14,
    borderRadius: 14,
    borderWidth: 1,
    marginBottom: 12,
  },

  player: {
    fontSize: 15,
    fontWeight: "900",
    marginBottom: 2,
  },

  context: {
    fontSize: 12,
    fontWeight: "700",
    marginBottom: 6,
  },

  title: {
    fontSize: 14,
    fontWeight: "900",
  },

  body: {
    marginTop: 6,
    fontSize: 13,
    fontWeight: "600",
  },

  metricsRow: {
    flexDirection: "row",
    flexWrap: "wrap",
    gap: 10,
    marginTop: 6,
  },

  metric: {
    fontSize: 11,
    fontWeight: "700",
  },

  metricStrong: {
    fontSize: 11,
    fontWeight: "900",
  },

  expanded: {
    marginTop: 12,
    paddingTop: 12,
    borderTopWidth: 1,
  },

  expandedHeader: {
    fontSize: 12,
    fontWeight: "900",
    marginBottom: 4,
  },

  expandedRow: {
    flexDirection: "row",
    justifyContent: "space-between",
    marginBottom: 10,
  },

  cell: {
    fontSize: 12,
    fontWeight: "700",
  },

  risk: {
    fontSize: 12,
    fontWeight: "800",
  },

  meta: {
    marginTop: 8,
    fontSize: 11,
    fontWeight: "600",
  },
});