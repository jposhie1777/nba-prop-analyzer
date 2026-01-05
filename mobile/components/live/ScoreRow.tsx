import { View, Text, StyleSheet, Image } from "react-native";
import { useTheme } from "@/store/useTheme";
import { LiveGame } from "@/types/live";

export function ScoreRow({ game }: { game: LiveGame }) {
  const { colors } = useTheme();

  const hasLinescore =
    !!game.linescore?.home?.length && !!game.linescore?.away?.length;

  const quarters = Math.max(
    game.linescore?.home?.length ?? 0,
    game.linescore?.away?.length ?? 0
  );

  return (
    <View style={styles.wrap}>
      {/* Big score row */}
      <View style={styles.topRow}>
        <View style={styles.sideCol}>
          <Text style={[styles.bigScore, { color: colors.text.primary }]}>
            {game.home.score}
          </Text>

          <View style={styles.teamRow}>
            <Image source={{ uri: game.home.logo }} style={styles.logo} />
            <Text style={[styles.abbrev, { color: colors.text.secondary }]}>
              {game.home.abbrev}
            </Text>
          </View>
        </View>

        <View style={styles.midCol}>
          <View style={[styles.livePill, { backgroundColor: colors.surface.cardSoft}]}>
            <Text style={[styles.liveText, { color: colors.text.secondary }]}>
              LIVE
            </Text>
          </View>

          <Text style={[styles.status, { color: colors.text.muted }]}>
            {game.period} • {game.clock}
          </Text>
        </View>

        <View style={styles.sideCol}>
          <Text style={[styles.bigScore, { color: colors.text.primary }]}>
            {game.away.score}
          </Text>

          <View style={styles.teamRowRight}>
            <Text style={[styles.abbrev, { color: colors.text.secondary }]}>
              {game.away.abbrev}
            </Text>
            <Image source={{ uri: game.away.logo }} style={styles.logo} />
          </View>
        </View>
      </View>

      {/* Quarter linescore */}
      {hasLinescore && (
        <View style={[styles.linescoreBox, { borderColor: colors.border.subtle }]}>
          <View style={styles.lineHeaderRow}>
            <Text style={[styles.lineHeaderLeft, { color: colors.text.muted }]}>
              {/* spacer for team labels */}
            </Text>

            {Array.from({ length: quarters }).map((_, i) => (
              <Text
                key={`q-${i}`}
                style={[styles.lineHeaderCell, { color: colors.text.muted }]}
              >
                {i + 1}
              </Text>
            ))}

            <Text style={[styles.lineHeaderCell, { color: colors.text.muted }]}>
              T
            </Text>
          </View>

          <View style={styles.lineTeamRow}>
            <Text style={[styles.lineTeamLabel, { color: colors.text.secondary }]}>
              {game.home.abbrev}
            </Text>

            {Array.from({ length: quarters }).map((_, i) => (
              <Text
                key={`h-${i}`}
                style={[styles.lineCell, { color: colors.text.muted }]}
              >
                {game.linescore?.home?.[i] ?? "—"}
              </Text>
            ))}

            <Text style={[styles.lineCell, { color: colors.text.secondary }]}>
              {game.home.score}
            </Text>
          </View>

          <View style={styles.lineTeamRow}>
            <Text style={[styles.lineTeamLabel, { color: colors.text.secondary }]}>
              {game.away.abbrev}
            </Text>

            {Array.from({ length: quarters }).map((_, i) => (
              <Text
                key={`a-${i}`}
                style={[styles.lineCell, { color: colors.text.muted }]}
              >
                {game.linescore?.away?.[i] ?? "—"}
              </Text>
            ))}

            <Text style={[styles.lineCell, { color: colors.text.secondary }]}>
              {game.away.score}
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

  sideCol: { width: "38%" },
  midCol: { width: "24%", alignItems: "center", gap: 6 },

  bigScore: {
    fontSize: 52,
    fontWeight: "800",
    letterSpacing: -1,
    textAlign: "center",
  },

  teamRow: {
    flexDirection: "row",
    justifyContent: "center",
    alignItems: "center",
    gap: 8,
  },
  teamRowRight: {
    flexDirection: "row",
    justifyContent: "center",
    alignItems: "center",
    gap: 8,
  },

  logo: { width: 26, height: 26, resizeMode: "contain" },
  abbrev: { fontSize: 14, fontWeight: "700" },

  livePill: {
    paddingHorizontal: 10,
    paddingVertical: 4,
    borderRadius: 999,
  },
  liveText: { fontSize: 12, fontWeight: "800", letterSpacing: 1 },

  status: { fontSize: 12, fontWeight: "600" },

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
  lineHeaderCell: { flex: 1, textAlign: "center", fontSize: 11, fontWeight: "700" },

  lineTeamRow: { flexDirection: "row", alignItems: "center", paddingVertical: 4 },
  lineTeamLabel: { width: 44, fontSize: 12, fontWeight: "800" },
  lineCell: { flex: 1, textAlign: "center", fontSize: 12, fontWeight: "700" },
});
