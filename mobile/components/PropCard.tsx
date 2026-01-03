import { View, Text, StyleSheet, Image } from "react-native";
import { Swipeable } from "react-native-gesture-handler";
import { useEffect, useRef } from "react";
import * as Haptics from "expo-haptics";
import Animated, {
  useSharedValue,
  useAnimatedStyle,
  withSpring,
  withTiming,
} from "react-native-reanimated";

import colors from "../theme/color";
import textStyles from "../theme/text";
import { BOOKMAKER_LOGOS } from "../utils/bookmakerLogos";

// ---------------------------
// TEAM LOGOS
// ---------------------------
const TEAM_LOGOS: Record<string, string> = {
  ATL: "https://a.espncdn.com/i/teamlogos/nba/500/atl.png",
  BOS: "https://a.espncdn.com/i/teamlogos/nba/500/bos.png",
  BKN: "https://a.espncdn.com/i/teamlogos/nba/500/bkn.png",
  CHA: "https://a.espncdn.com/i/teamlogos/nba/500/cha.png",
  CHI: "https://a.espncdn.com/i/teamlogos/nba/500/chi.png",
  CLE: "https://a.espncdn.com/i/teamlogos/nba/500/cle.png",
  DAL: "https://a.espncdn.com/i/teamlogos/nba/500/dal.png",
  DEN: "https://a.espncdn.com/i/teamlogos/nba/500/den.png",
  DET: "https://a.espncdn.com/i/teamlogos/nba/500/det.png",
  GSW: "https://a.espncdn.com/i/teamlogos/nba/500/gsw.png",
  HOU: "https://a.espncdn.com/i/teamlogos/nba/500/hou.png",
  IND: "https://a.espncdn.com/i/teamlogos/nba/500/ind.png",
  LAC: "https://a.espncdn.com/i/teamlogos/nba/500/lac.png",
  LAL: "https://a.espncdn.com/i/teamlogos/nba/500/lal.png",
  MEM: "https://a.espncdn.com/i/teamlogos/nba/500/mem.png",
  MIA: "https://a.espncdn.com/i/teamlogos/nba/500/mia.png",
  MIL: "https://a.espncdn.com/i/teamlogos/nba/500/mil.png",
  MIN: "https://a.espncdn.com/i/teamlogos/nba/500/min.png",
  NOP: "https://a.espncdn.com/i/teamlogos/nba/500/nop.png",
  NYK: "https://a.espncdn.com/i/teamlogos/nba/500/nyk.png",
  OKC: "https://a.espncdn.com/i/teamlogos/nba/500/okc.png",
  ORL: "https://a.espncdn.com/i/teamlogos/nba/500/orl.png",
  PHI: "https://a.espncdn.com/i/teamlogos/nba/500/phi.png",
  PHX: "https://a.espncdn.com/i/teamlogos/nba/500/phx.png",
  POR: "https://a.espncdn.com/i/teamlogos/nba/500/por.png",
  SAC: "https://a.espncdn.com/i/teamlogos/nba/500/sac.png",
  SAS: "https://a.espncdn.com/i/teamlogos/nba/500/sas.png",
  TOR: "https://a.espncdn.com/i/teamlogos/nba/500/tor.png",
  UTA: "https://a.espncdn.com/i/teamlogos/nba/500/uta.png",
  WAS: "https://a.espncdn.com/i/teamlogos/nba/500/was.png",
};

// ---------------------------
// PROPS
// ---------------------------
type BookOdds = {
  bookmaker: string;
  odds: number;
};

type PropCardProps = {
  player: string;
  market: string;
  line: number;
  odds: number;

  hitRateL10: number;
  edge: number;
  confidence: number;

  matchup?: string;
  home?: string;
  away?: string;

  bookmaker?: string;

  // 🔥 MULTI-BOOK (NEW, OPTIONAL)
  books?: BookOdds[];

  saved: boolean;
  onToggleSave: () => void;
};

export default function PropCard({
  player,
  market,
  line,
  odds,
  hitRateL10,
  confidence,
  matchup,
  home,
  away,
  bookmaker,
  books,
  saved,
  onToggleSave,
}: PropCardProps) {
  const hitPct = Math.round(hitRateL10 * 100);

  const confidenceColor =
    confidence >= 75
      ? colors.success
      : confidence >= 60
      ? colors.accent
      : colors.textSecondary;

  // ---------------------------
  // MULTI-BOOK NORMALIZATION
  // ---------------------------
  const resolvedBooks: BookOdds[] =
    books && books.length > 0
      ? books
      : bookmaker
      ? [{ bookmaker, odds }]
      : [];

  // ---------------------------
  // ODDS CHANGE ANIMATION
  // ---------------------------
  const prevOddsRef = useRef<Record<string, number>>({});
  const flash = useSharedValue(0);

  useEffect(() => {
    let changed = false;

    resolvedBooks.forEach(({ bookmaker, odds }) => {
      const key = bookmaker.toLowerCase().replace(/[\s_]/g, "");
      const prev = prevOddsRef.current[key];

      if (prev !== undefined && prev !== odds) {
        changed = true;
      }

      prevOddsRef.current[key] = odds;
    });

    if (changed) {
      flash.value = withTiming(1, { duration: 120 }, () => {
        flash.value = withTiming(0, { duration: 500 });
      });
    }
  }, [resolvedBooks]);

  const flashStyle = useAnimatedStyle(() => ({
    backgroundColor:
      flash.value > 0
        ? "rgba(61,255,181,0.12)"
        : "transparent",
  }));

  // ---------------------------
  // SAVE ANIMATION
  // ---------------------------
  const scale = useSharedValue(1);
  const opacity = useSharedValue(1);

  const animatedStyle = useAnimatedStyle(() => ({
    transform: [{ scale: scale.value }],
    opacity: opacity.value,
  }));

  useEffect(() => {
    if (saved) {
      scale.value = withSpring(1.08, { damping: 12 });
      scale.value = withSpring(1);
    } else {
      opacity.value = withTiming(0.7, { duration: 120 });
      opacity.value = withTiming(1, { duration: 160 });
    }
  }, [saved]);

  const handleToggleSave = () => {
    Haptics.impactAsync(
      saved
        ? Haptics.ImpactFeedbackStyle.Light
        : Haptics.ImpactFeedbackStyle.Medium
    );
    onToggleSave();
  };

  return (
    <Swipeable
      overshootRight={false}
      onSwipeableOpen={handleToggleSave}
    >
      <Animated.View style={animatedStyle}>
        <Animated.View style={[styles.card, flashStyle]}>
          {/* SAVE STAR */}
          <View style={styles.saveButton}>
            <Text
              onPress={handleToggleSave}
              style={{
                color: saved ? colors.accent : colors.textSecondary,
                fontSize: 18,
                fontWeight: "700",
              }}
            >
              {saved ? "★" : "☆"}
            </Text>
          </View>

          {/* HEADER */}
          <View style={styles.headerRow}>
            <View style={styles.matchupLeft}>
              {away && TEAM_LOGOS[away] && (
                <Image source={{ uri: TEAM_LOGOS[away] }} style={styles.teamLogo} />
              )}
              {home && TEAM_LOGOS[home] && (
                <Image source={{ uri: TEAM_LOGOS[home] }} style={styles.teamLogo} />
              )}
            </View>

            <View style={styles.headerCenter}>
              <Text style={styles.player}>{player}</Text>
              <Text style={styles.marketCenter}>
                {market} • {line}
              </Text>
              {matchup && (
                <Text style={styles.matchupText}>{matchup}</Text>
              )}
            </View>

            {/* MULTI BOOK ODDS */}
            <View style={styles.headerRight}>
              {resolvedBooks.map((b) => {
                const key = b.bookmaker
                  .toLowerCase()
                  .replace(/[\s_]/g, "");

                return (
                  <View key={key} style={styles.oddsRow}>
                    {BOOKMAKER_LOGOS[key] && (
                      <Image
                        source={BOOKMAKER_LOGOS[key]}
                        style={styles.bookLogo}
                        resizeMode="contain"
                      />
                    )}
                    <Text style={styles.oddsTop}>
                      {b.odds > 0 ? `+${b.odds}` : b.odds}
                    </Text>
                  </View>
                );
              })}
            </View>
          </View>

          {/* METRICS */}
          <View style={styles.row}>
            <Text style={styles.hit}>{hitPct}% HIT</Text>
          </View>

          {/* CONFIDENCE */}
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
        </Animated.View>
      </Animated.View>
    </Swipeable>
  );
}

// ---------------------------
// STYLES
// ---------------------------
const styles = StyleSheet.create({
  card: {
    backgroundColor: colors.card,
    borderRadius: 20,
    padding: 16,
    marginHorizontal: 16,
    marginVertical: 12,
  },
  saveButton: {
    position: "absolute",
    top: 10,
    right: 12,
    zIndex: 10,
  },
  headerRow: {
    flexDirection: "row",
    alignItems: "center",
    marginBottom: 10,
  },
  matchupLeft: {
    width: 60,
    flexDirection: "row",
    gap: 6,
  },
  teamLogo: {
    width: 22,
    height: 22,
  },
  headerCenter: {
    flex: 1,
    alignItems: "center",
  },
  headerRight: {
    width: 90,
    alignItems: "flex-end",
  },
  oddsRow: {
    flexDirection: "row",
    alignItems: "center",
    gap: 4,
    marginBottom: 2,
  },
  bookLogo: {
    width: 16,
    height: 16,
  },
  player: {
    color: colors.textPrimary,
    fontSize: textStyles.title,
    fontWeight: "600",
  },
  marketCenter: {
    color: colors.textSecondary,
    fontSize: textStyles.subtitle,
  },
  matchupText: {
    color: colors.textSecondary,
    fontSize: textStyles.label,
    marginTop: 2,
  },
  oddsTop: {
    color: colors.textSecondary,
    fontSize: textStyles.label,
    fontWeight: "600",
  },
  row: {
    flexDirection: "row",
    justifyContent: "space-between",
    marginBottom: 10,
  },
  hit: {
    color: colors.success,
    fontSize: textStyles.stat,
    fontWeight: "600",
  },
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
});
