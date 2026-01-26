import { View, Text, StyleSheet } from "react-native";
import { useMemo } from "react";

import { useTheme } from "@/store/useTheme";
import { TrackedParlaySnapshot } from "@/store/useParlayTracker";
import { calcLegProgress } from "@/utils/parlayProgress";
import LegProgressBar from "@/components/tracked/LegProgressBar";

/* ======================================================
   TYPES
====================================================== */

type Props = {
  parlay: TrackedParlaySnapshot;
};

/* ======================================================
   COMPONENT
====================================================== */

export default function TrackedParlayCard({ parlay }: Props) {
  const colors = useTheme((s) => s.colors);
  const styles = useMemo(() => makeStyles(colors), [colors]);

  /* ======================================================
     PARLAY SUMMARY
  ====================================================== */
  const summary = useMemo(() => {
    const total = parlay.legs.length;
    const winning = parlay.legs.filter((l) => l.status === "winning").length;
    const losing = parlay.legs.filter((l) => l.status === "losing").length;
    const finals = parlay.legs.filter((l) => l.isFinal).length;

    if (losing > 0) {
      return { label: "Danger", color: colors.accent.danger };
    }

    if (finals === total && winning === total && total > 0) {
      return { label: "Won", color: colors.accent.success };
    }

    if (winning === total && total > 0) {
      return { label: "All Winning", color: colors.accent.success };
    }

    return { label: "Sweating", color: colors.accent.primary };
  }, [parlay.legs, colors]);

  return (
    <View style={styles.card}>
      {/* ================= HEADER ================= */}
      <View style={styles.header}>
        <Text style={styles.source}>
          {String(parlay.source).toUpperCase()}
        </Text>

        <Text style={[styles.state, { color: summary.color }]}>
          {summary.label}
        </Text>
      </View>

      {/* ================= LEGS ================= */}
      <View style={styles.legs}>
        {parlay.legs.map((leg) => {
          const progress = calcLegProgress(
            leg.current,
            leg.line,
            leg.side
          );

          return (
            <View key={leg.leg_id} style={styles.leg}>
              {/* ---------- LINE ---------- */}
              <View style={styles.legRow}>
                <Text style={styles.legText} numberOfLines={1}>
                  {leg.player_name}{" "}
                  {leg.side.toUpperCase()} {leg.line}{" "}
                  {leg.market.toUpperCase()}
                </Text>

                {/* RIGHT SIDE: STAT + CLOCK */}
                <View style={styles.legRight}>
                  <Text
                    style={[
                      styles.legValue,
                      leg.status === "winning" && {
                        color: colors.accent.success,
                      },
                      leg.status === "losing" && {
                        color: colors.accent.danger,
                      },
                    ]}
                  >
                    {leg.current ?? "—"}
                  </Text>

                  {(leg.period != null || leg.clock) && (
                    <Text style={styles.clock}>
                      Q{leg.period ?? "—"} {leg.clock ?? ""}
                    </Text>
                  )}
                </View>
              </View>

              {/* ---------- PROGRESS ---------- */}
              <LegProgressBar
                progress={progress}
                status={leg.status}
                isFinal={leg.isFinal}
              />
            </View>
          );
        })}
      </View>

      {/* ================= FOOTER ================= */}
      <View style={styles.footer}>
        <Text style={styles.footerText}>
          Stake: ${parlay.stake.toFixed(2)}
        </Text>

        <Text style={styles.footerText}>
          To Win: ${parlay.payout?.toFixed(2) ?? "—"}
        </Text>
      </View>
    </View>
  );
}

/* ======================================================
   STYLES
====================================================== */

function makeStyles(colors: any) {
  return StyleSheet.create({
    card: {
      backgroundColor: colors.surface.card,
      borderRadius: 14,
      padding: 14,
      marginBottom: 12,
      borderWidth: 1,
      borderColor: colors.border.subtle,
    },

    header: {
      flexDirection: "row",
      justifyContent: "space-between",
      alignItems: "center",
      marginBottom: 12,
    },

    source: {
      fontSize: 12,
      fontWeight: "700",
      letterSpacing: 0.5,
      color: colors.text.secondary,
    },

    state: {
      fontSize: 12,
      fontWeight: "600",
    },

    legs: {
      gap: 14,
    },

    leg: {
      gap: 6,
    },

    legRow: {
      flexDirection: "row",
      justifyContent: "space-between",
      alignItems: "center",
    },

    legText: {
      fontSize: 13,
      color: colors.text.primary,
      flexShrink: 1,
      paddingRight: 8,
    },

    legRight: {
      alignItems: "flex-end",
    },

    legValue: {
      fontSize: 13,
      fontWeight: "700",
      color: colors.text.primary,
    },

    clock: {
      fontSize: 10,
      marginTop: 2,
      color: colors.text.muted,
      fontWeight: "600",
    },

    footer: {
      marginTop: 14,
      paddingTop: 10,
      borderTopWidth: 1,
      borderTopColor: colors.border.subtle,
      flexDirection: "row",
      justifyContent: "space-between",
    },

    footerText: {
      fontSize: 12,
      color: colors.text.muted,
    },
  });
} by