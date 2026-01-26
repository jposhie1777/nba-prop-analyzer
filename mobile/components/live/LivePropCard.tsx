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

function formatOdds(odds?: number) {
  if (odds == null) return "";
  return odds > 0 ? `+${odds}` : `${odds}`;
}

/* =====================================================
   CARD
===================================================== */
export default function LivePropCard({ item }: { item: any }) {
  const { colors } = useTheme();
  const [expanded, setExpanded] = useState(false);

  /* -----------------------------
     CONTEXT
  ------------------------------ */
  const contextText =
    item.game_state === "halftime"
      ? "Halftime"
      : item.game_period && item.game_clock
      ? `${item.away_team_abbr ?? "AWY"} vs ${
          item.home_team_abbr ?? "HOM"
        } · ${item.game_period} ${item.game_clock}`
      : `${item.away_team_abbr ?? "AWY"} vs ${
          item.home_team_abbr ?? "HOM"
        }`;

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
  let blowoutStyle = styles.pillWarning;

  if (item.score_margin >= 16) {
    blowoutLabel = "Blowout Risk";
    blowoutStyle = styles.pillDanger;
  } else if (item.score_margin >= 10) {
    blowoutLabel = "Blowout Watch";
  }

  return (
    <Pressable
      onPress={() => setExpanded((v) => !v)}
      style={[
        styles.card,
        {
          backgroundColor: colors.surface.card,
          shadowColor: "#000",
        },
      ]}
    >
      {/* =========================
          HEADER
      ========================== */}
      <View style={styles.headerRow}>
        {/* Headshot */}
        <View
          style={[
            styles.headshot,
            { backgroundColor: colors.surface.cardSoft },
          ]}
        />

        <View style={{ flex: 1 }}>
          <Text style={[styles.player, { color: colors.text.primary }]}>
            {item.player_name ?? "Unknown Player"}
          </Text>

          <Text style={[styles.subtle, { color: colors.text.muted }]}>
            {contextText}
          </Text>
        </View>

        {projectedFinal && (
          <View style={[styles.pill, styles.pillNeutral]}>
            <Text style={styles.pillText}>
              Pace {projectedFinal.toFixed(1)}
            </Text>
          </View>
        )}
      </View>

      {/* =========================
          MARKET
      ========================== */}
      <View style={styles.marketRow}>
        <Text style={[styles.title, { color: colors.text.primary }]}>
          {item.market.toUpperCase()} · {item.line}
        </Text>

        {blowoutLabel && (
          <View style={[styles.pill, blowoutStyle]}>
            <Text style={styles.pillText}>{blowoutLabel}</Text>
          </View>
        )}
      </View>

      <Text style={[styles.body, { color: colors.text.secondary }]}>
        Current {item.current_stat} → Need {item.remaining_needed}
      </Text>

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
          <Text style={[styles.expandedHeader, { color: colors.text.primary }]}>
            Current
          </Text>
          <View style={styles.expandedRow}>
            <Text style={styles.cell}>Projected: 28.4</Text>
            <Text style={styles.cell}>Minutes: 36.1</Text>
          </View>

          <Text style={[styles.expandedHeader, { color: colors.text.primary }]}>
            Historical H2
          </Text>
          <View style={styles.expandedRow}>
            <Text style={styles.cell}>L5: 11.8</Text>
            <Text style={styles.cell}>L10: 10.9</Text>
            <Text style={styles.cell}>Min L5: 17.4</Text>
          </View>
        </View>
      )}

      {/* =========================
          FOOTER
      ========================== */}
      <View style={styles.footerRow}>
        <View
          style={[
            styles.bookPill,
            { backgroundColor: colors.surface.elevated },
          ]}
        >
          <Text style={styles.bookText}>
            {item.book === "fanduel"
              ? "FD"
              : item.book === "draftkings"
              ? "DK"
              : "BK"}
          </Text>
        </View>

        {item.over_odds != null && (
          <Text style={[styles.odds, { color: colors.text.primary }]}>
            {formatOdds(item.over_odds)}
          </Text>
        )}
      </View>
    </Pressable>
  );
}

/* =====================================================
   STYLES
===================================================== */
const styles = StyleSheet.create({
  card: {
    padding: 16,
    borderRadius: 18,
    marginBottom: 14,
    shadowOpacity: 0.04,
    shadowRadius: 14,
    shadowOffset: { width: 0, height: 6 },
  },

  headerRow: {
    flexDirection: "row",
    alignItems: "center",
    gap: 12,
  },

  headshot: {
    width: 44,
    height: 44,
    borderRadius: 22,
  },

  player: {
    fontSize: 16,
    fontWeight: "900",
  },

  subtle: {
    fontSize: 12,
    fontWeight: "600",
    marginTop: 2,
  },

  marketRow: {
    flexDirection: "row",
    alignItems: "center",
    justifyContent: "space-between",
    marginTop: 12,
  },

  title: {
    fontSize: 15,
    fontWeight: "900",
  },

  body: {
    marginTop: 6,
    fontSize: 12,
    fontWeight: "600",
    opacity: 0.85,
  },

  pill: {
    paddingHorizontal: 8,
    paddingVertical: 4,
    borderRadius: 999,
  },

  pillText: {
    fontSize: 11,
    fontWeight: "800",
    color: "#111",
  },

  pillNeutral: {
    backgroundColor: "#EEF2FF",
  },

  pillWarning: {
    backgroundColor: "#FEF3C7",
  },

  pillDanger: {
    backgroundColor: "#FEE2E2",
  },

  expanded: {
    marginTop: 14,
    paddingTop: 12,
    borderTopWidth: 1,
  },

  expandedHeader: {
    fontSize: 12,
    fontWeight: "900",
    marginBottom: 6,
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

  footerRow: {
    flexDirection: "row",
    alignItems: "center",
    gap: 8,
    marginTop: 12,
  },

  bookPill: {
    minWidth: 34,
    height: 22,
    borderRadius: 7,
    alignItems: "center",
    justifyContent: "center",
    paddingHorizontal: 6,
  },

  bookText: {
    fontSize: 11,
    fontWeight: "900",
  },

  odds: {
    fontSize: 14,
    fontWeight: "900",
  },
});