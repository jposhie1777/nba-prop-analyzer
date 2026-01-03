import { View, Text, StyleSheet } from "react-native";
import { Swipeable } from "react-native-gesture-handler";
import colors from "../theme/color";
import textStyles from "../theme/text";

type PropCardProps = {
  player: string;
  market: string;

  line: number;
  odds: number;

  hitRate: number; // 0–1
  edge: number;    // 0–1

  home: string;
  away: string;

  confidence?: number;

  // SAVE BET
  saved?: boolean;
  onToggleSave?: () => void;
};

export default function PropCard({
  player,
  market,
  line,
  odds,
  hitRate,
  edge,
  home,
  away,
  confidence,
  saved = false,
  onToggleSave,
}: PropCardProps) {
  const hitPct = Math.round(hitRate * 100);
  const edgePct = Math.round(edge * 100);
  const oddsLabel = odds > 0 ? `+${odds}` : `${odds}`;

  const isPositiveEV = edge >= 0.1;   // 10%+
  const isNeutralEV = edge >= 0.05;   // 5–10%

  const confidenceColor =
    confidence !== undefined && confidence >= 75
      ? colors.success
      : confidence !== undefined && confidence >= 60
      ? colors.accent
      : colors.textSecondary;

  // ---------------------------
  // SWIPE ACTION (RIGHT)
  // ---------------------------
  const renderRightActions = () => (
    <View style={styles.swipeAction}>
      <Text style={styles.swipeText}>
        {saved ? "Unsave" : "Save"}
      </Text>
    </View>
  );

  return (
    <Swipeable
      renderRightActions={renderRightActions}
      onSwipeableOpen={onToggleSave}
      overshootRight={false}
    >
      <View style={styles.card}>
        {/* STAR SAVE BUTTON */}
        <View style={styles.saveButton}>
          <Text
            onPress={onToggleSave}
            style={{
              color: saved ? colors.accent : colors.textSecondary,
              fontSize: 18,
              fontWeight: "700",
            }}
          >
            {saved ? "★" : "☆"}
          </Text>
        </View>

        {/* Matchup */}
        <Text style={styles.matchup}>
          {away} @ {home}
        </Text>

        {/* Player */}
        <Text style={styles.player}>{player}</Text>

        {/* Market + Odds */}
        <View style={styles.marketRow}>
          <Text style={styles.market}>
            {market} • {line}
          </Text>

          <Text style={styles.odds}>{oddsLabel}</Text>
        </View>

        {/* Decision row */}
        <View style={styles.row}>
          <Text style={styles.hit}>{hitPct}% HIT</Text>

          <View style={{ flexDirection: "row", alignItems: "center" }}>
            <Text style={styles.edge}>+{edgePct}% EDGE</Text>

            <View
              style={[
                styles.evBadge,
                isPositiveEV
                  ? styles.evPositive
                  : isNeutralEV
                  ? styles.evNeutral
                  : styles.evNegative,
              ]}
            >
              <Text
                style={[
                  styles.evText,
                  {
                    color: isPositiveEV
                      ? colors.success
                      : isNeutralEV
                      ? colors.accent
                      : colors.textSecondary,
                  },
                ]}
              >
                {isPositiveEV ? "+EV" : isNeutralEV ? "EV" : "LOW"}
              </Text>
            </View>
          </View>
        </View>

        {/* Divider */}
        <View style={styles.divider} />

        {/* Confidence */}
        {confidence !== undefined && (
          <View style={styles.confidenceRow}>
            <View style={styles.confidenceBar}>
              <View
                style={[
                  styles.confidenceFill,
                  {
                    width: `${confidence}%`,
                    backgroundColor: confidenceColor,
                  },
                ]}
              />
            </View>

            <Text style={styles.confidenceLabel}>{confidence}</Text>
          </View>
        )}
      </View>
    </Swipeable>
  );
}

const styles = StyleSheet.create({
  card: {
    backgroundColor: colors.card,
    borderRadius: 20,
    padding: 16,
    marginHorizontal: 16,
    marginVertical: 12,
    position: "relative",
  },

  // ---------- SAVE ----------
  saveButton: {
    position: "absolute",
    top: 10,
    right: 12,
    zIndex: 10,
  },

  swipeAction: {
    backgroundColor: colors.accent,
    justifyContent: "center",
    alignItems: "center",
    width: 90,
    marginVertical: 12,
    borderRadius: 20,
  },

  swipeText: {
    color: colors.bg,
    fontWeight: "700",
    fontSize: textStyles.label,
  },

  // ---------- TEXT ----------
  matchup: {
    color: colors.textSecondary,
    fontSize: textStyles.label,
    marginBottom: 6,
  },

  player: {
    color: colors.textPrimary,
    fontSize: textStyles.title,
    fontWeight: "600",
    marginBottom: 2,
  },

  marketRow: {
    flexDirection: "row",
    justifyContent: "space-between",
    alignItems: "center",
    marginBottom: 12,
  },

  market: {
    color: colors.textSecondary,
    fontSize: textStyles.subtitle,
  },

  odds: {
    color: colors.textSecondary,
    fontSize: textStyles.label,
    fontWeight: "600",
  },

  row: {
    flexDirection: "row",
    justifyContent: "space-between",
    alignItems: "center",
    marginBottom: 10,
  },

  hit: {
    color: colors.success,
    fontSize: textStyles.stat,
    fontWeight: "600",
  },

  edge: {
    color: colors.accent,
    fontSize: textStyles.stat,
    fontWeight: "600",
  },

  divider: {
    height: 1,
    backgroundColor: colors.divider,
    marginVertical: 8,
  },

  // ---------- CONFIDENCE ----------
  confidenceRow: {
    flexDirection: "row",
    alignItems: "center",
    gap: 10,
  },

  confidenceBar: {
    flex: 1,
    height: 6,
    borderRadius: 4,
    backgroundColor: "rgba(255,255,255,0.08)",
    overflow: "hidden",
  },

  confidenceFill: {
    height: "100%",
    borderRadius: 4,
  },

  confidenceLabel: {
    color: colors.textSecondary,
    fontSize: textStyles.label,
    width: 32,
    textAlign: "right",
  },

  // ---------- EV ----------
  evBadge: {
    paddingHorizontal: 8,
    paddingVertical: 3,
    borderRadius: 6,
    marginLeft: 8,
  },

  evPositive: {
    backgroundColor: "rgba(61,255,181,0.15)",
  },

  evNeutral: {
    backgroundColor: "rgba(108,124,255,0.15)",
  },

  evNegative: {
    backgroundColor: "rgba(255,255,255,0.08)",
  },

  evText: {
    fontSize: textStyles.label,
    fontWeight: "600",
  },
});