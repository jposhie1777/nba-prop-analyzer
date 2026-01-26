import { View, Text, StyleSheet, Pressable } from "react-native";
import { useTheme } from "@/store/useTheme";
import { useState } from "react";

/* ---------------------------------
   Pace helpers
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

export default function LivePropCard({ item }: { item: any }) {
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
     PACE
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

  const projectedFinal =
    progress && progress > 0.05
      ? item.current_stat / progress
      : null;

  /* -----------------------------
     BLOWOUT
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
          HEADER ROW
      ========================== */}
      <View style={styles.headerRow}>
        {/* Headshot placeholder */}
        <View
          style={[
            styles.headshot,
            { backgroundColor: colors.surface.cardSoft },
          ]}
        />

        <View style={{ flex: 1 }}>
          {/* Player */}
          <Text style={[styles.player, { color: colors.text.primary }]}>
            {item.player_name ?? "Unknown Player"}
          </Text>

          {/* Matchup */}
          <View style={styles.matchupRow}>
            <View
              style={[
                styles.logo,
                { backgroundColor: colors.surface.elevated },
              ]}
            />
            <Text
              style={[
                styles.matchupText,
                { color: colors.text.muted },
              ]}
            >
              {item.away_team_abbr ?? "AWY"} vs{" "}
              {item.home_team_abbr ?? "HOM"}
            </Text>
            <View
              style={[
                styles.logo,
                { backgroundColor: colors.surface.elevated },
              ]}
            />
          </View>

          {/* Game context */}
          {gameContext && (
            <Text
              style={[
                styles.context,
                { color: colors.text.muted },
              ]}
            >
              {gameContext}
            </Text>
          )}
        </View>
      </View>

      {/* =========================
          MARKET
      ========================== */}
      <Text style={[styles.title, { color: colors.text.primary }]}>
        {item.market.toUpperCase()} · {item.line}
      </Text>

      <Text style={[styles.body, { color: colors.text.secondary }]}>
        Current: {item.current_stat} → Need {item.remaining_needed}
      </Text>

      {/* =========================
          METRICS
      ========================== */}
      <View style={styles.metricsRow}>
        {projectedFinal && (
          <Text
            style={[
              styles.metric,
              { color: colors.text.secondary },
            ]}
          >
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
          EXPANDED (layout only)
      ========================== */}
      {expanded && (
        <View
          style={[
            styles.expanded,
            { borderColor: colors.border.subtle },
          ]}
        >
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
        </View>
      )}

      {/* Book */}
      <Text style={[styles.meta, { color: colors.text.muted }]}>
        Book: {item.book}
      </Text>
    </Pressable>
  );
}

const styles = StyleSheet.create({
  card: {
    padding: 14,
    borderRadius: 16,
    borderWidth: 1,
    marginBottom: 12,
  },

  /* Header */
  headerRow: {
    flexDirection: "row",
    gap: 12,
    marginBottom: 8,
  },

  headshot: {
    width: 48,
    height: 48,
    borderRadius: 24,
  },

  player: {
    fontSize: 15,
    fontWeight: "900",
  },

  matchupRow: {
    flexDirection: "row",
    alignItems: "center",
    gap: 6,
    marginTop: 2,
  },

  logo: {
    width: 14,
    height: 14,
    borderRadius: 7,
  },

  matchupText: {
    fontSize: 11,
    fontWeight: "700",
  },

  context: {
    fontSize: 12,
    fontWeight: "700",
    marginTop: 2,
  },

  /* Body */
  title: {
    fontSize: 14,
    fontWeight: "900",
    marginTop: 6,
  },

  body: {
    marginTop: 6,
    fontSize: 13,
    fontWeight: "600",
  },

  metricsRow: {
    flexDirection: "row",
    gap: 12,
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

  /* Expanded */
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

  meta: {
    marginTop: 8,
    fontSize: 11,
    fontWeight: "600",
  },
});