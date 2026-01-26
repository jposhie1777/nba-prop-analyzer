import { View, Text, StyleSheet, Pressable } from "react-native";
import { useTheme } from "@/store/useTheme";
import { useState } from "react";

/* =====================================================
   PACE HELPERS (pure utilities)
===================================================== */
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

/* =====================================================
   LIVE PROP CARD
===================================================== */
type LivePropCardProps = {
  item: any;
};

export default function LivePropCard({ item }: LivePropCardProps) {
  const { colors } = useTheme();
  const [expanded, setExpanded] = useState(false);

  /* -----------------------------
     GAME CONTEXT
  ------------------------------ */
  const gameContext =
    item.game_state === "halftime"
      ? "Halftime"
      : item.game_period && item.game_clock
      ? `${item.game_period} · ${item.game_clock}`
      : null;

  /* -----------------------------
     PACE CALCULATION
  ------------------------------ */
  const elapsedMinutes = getElapsedMinutes(
    item.game_period,
    item.game_clock,
    item.game_state
  );

  const progress =
    elapsedMinutes && elapsedMinutes > 0
      ? elapsedMinutes / 48
      : null;

  // Guardrail: ignore very early game noise
  const projectedFinal =
    progress && progress > 0.05
      ? item.current_stat / progress
      : null;

  /* -----------------------------
     BLOWOUT RISK
  ------------------------------ */
  let blowoutLabel: string | null = null;
  let blowoutColor = colors.text.secondary;

  if (item.score_margin >= 16) {
    blowoutLabel = "Blowout Risk";
    blowoutColor = colors.accent.danger;
  } else if (item.score_margin >= 10) {
    blowoutLabel = "Blowout Watch";
    blowoutColor = colors.accent.warning;
  }

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

        {blowoutLabel && (
          <Text
            style={[
              styles.metricStrong,
              { color: blowoutColor },
            ]}
          >
            {blowoutLabel}
          </Text>
        )}
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
              Projected: {projectedFinal?.toFixed(1) ?? "—"}
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
          {blowoutLabel && (
            <>
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
                  { color: blowoutColor },
                ]}
              >
                ⚠ {blowoutLabel}
              </Text>
            </>
          )}
        </View>
      )}

      {/* Book */}
      <Text style={[styles.meta, { color: colors.text.muted }]}>
        Book: {item.book}
      </Text>
    </Pressable>
  );
}

/* =====================================================
   STYLES
===================================================== */
const styles = StyleSheet.create({
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