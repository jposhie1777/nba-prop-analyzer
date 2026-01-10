// components/live/ScoreRow
import { View, Text, StyleSheet } from "react-native";
import { useTheme } from "@/store/useTheme";
import { LiveGame } from "@/types/live";

export function ScoreRow({ game }: { game: LiveGame }) {
  const { colors } = useTheme();

  const hasLinescore = Array.isArray(game.lineScore);

  const quarters = game.lineScore?.length ?? 0;

  return (
    <View style={styles.wrap}>
      {/* ======================
          BIG SCORE ROW
      ====================== */}
      <View style={styles.topRow}>
        {/* Away */}
        <Text style={[styles.bigScore, { color: colors.text.primary }]}>
          {game.away.score ?? 0}
        </Text>

        {/* Center */}
        <View style={styles.midCol}>
          {/* Quarter */}
          {game.period && (
            <Text style={[styles.quarter, { color: colors.text.muted }]}>
              {game.period}
            </Text>
          )}
        
          {/* LIVE pill */}
          <View
            style={[
              styles.livePill,
              { backgroundColor: colors.surface.cardSoft },
            ]}
          >
            <Text
              style={[
                styles.liveText,
                { color: colors.text.secondary },
              ]}
            >
              LIVE
            </Text>
          </View>
        
          {/* Clock */}
          {game.clock && (
            <Text style={[styles.clock, { color: colors.text.muted }]}>
              {game.clock}
            </Text>
          )}
        </View>

        {/* Home */}
        <Text style={[styles.bigScore, { color: colors.text.primary }]}>
          {game.home.score ?? 0}
        </Text>
      </View>

      {/* ======================
          LINE SCORE
      ====================== */}
      {hasLinescore && (
        <View
          style={[
            styles.linescoreBox,
            { borderColor: colors.border.subtle },
          ]}
        >
          {/* Header */}
          <View style={styles.lineHeaderRow}>
            <Text style={styles.lineHeaderLeft} />

            {game.lineScore!.map((q) => (
              <Text
                key={q.label}
                style={[
                  styles.lineHeaderCell,
                  { color: colors.text.muted },
                ]}
              >
                {q.label}
              </Text>
            ))}

            <Text
              style={[
                styles.lineHeaderCell,
                { color: colors.text.muted },
              ]}
            >
              T
            </Text>
          </View>

          {/* Away row */}
          <View style={styles.lineTeamRow}>
            <Text
              style={[
                styles.lineTeamLabel,
                { color: colors.text.secondary },
              ]}
            >
              {game.away.abbrev}
            </Text>

            {game.lineScore!.map((q, i) => (
              <Text
                key={`away-${i}`}
                style={[
                  styles.lineCell,
                  { color: colors.text.muted },
                ]}
              >
                {q.away ?? "—"}
              </Text>
            ))}

            <Text
              style={[
                styles.lineCell,
                { color: colors.text.secondary },
              ]}
            >
              {game.away.score ?? 0}
            </Text>
          </View>

          {/* Home row */}
          <View style={styles.lineTeamRow}>
            <Text
              style={[
                styles.lineTeamLabel,
                { color: colors.text.secondary },
              ]}
            >
              {game.home.abbrev}
            </Text>

            {game.lineScore!.map((q, i) => (
              <Text
                key={`home-${i}`}
                style={[
                  styles.lineCell,
                  { color: colors.text.muted },
                ]}
              >
                {q.home ?? "—"}
              </Text>
            ))}

            <Text
              style={[
                styles.lineCell,
                { color: colors.text.secondary },
              ]}
            >
              {game.home.score ?? 0}
            </Text>
          </View>
        </View>
      )}
    </View>
  );
}

const styles = StyleSheet.create({
  wrap: { gap: 10 },

  topRow: {
    flexDirection: "row",
    alignItems: "center",
    justifyContent: "space-between",
  },

  bigScore: {
    fontSize: 52,
    fontWeight: "800",
    letterSpacing: -1,
    width: "38%",
    textAlign: "center",
  },

  midCol: {
    width: "24%",
    alignItems: "center",
    gap: 6,
  },
  quarter: {
    fontSize: 12,
    fontWeight: "700",
    letterSpacing: 0.5,
  },
  
  clock: {
    fontSize: 11,
    fontWeight: "600",
  },

  livePill: {
    paddingHorizontal: 10,
    paddingVertical: 4,
    borderRadius: 999,
  },

  liveText: {
    fontSize: 12,
    fontWeight: "800",
    letterSpacing: 1,
  },

  status: {
    fontSize: 12,
    fontWeight: "600",
  },

  linescoreBox: {
    borderWidth: 1,
    borderRadius: 12,
    paddingVertical: 10,
    paddingHorizontal: 12,
  },

  lineHeaderRow: {
    flexDirection: "row",
    alignItems: "center",
    paddingBottom: 8,
  },

  lineHeaderLeft: { width: 44 },

  lineHeaderCell: {
    flex: 1,
    textAlign: "center",
    fontSize: 11,
    fontWeight: "700",
  },

  lineTeamRow: {
    flexDirection: "row",
    alignItems: "center",
    paddingVertical: 4,
  },

  lineTeamLabel: {
    width: 44,
    fontSize: 12,
    fontWeight: "800",
  },

  lineCell: {
    flex: 1,
    textAlign: "center",
    fontSize: 12,
    fontWeight: "700",
  },
});