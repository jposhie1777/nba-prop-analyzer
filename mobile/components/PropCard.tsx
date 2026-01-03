import { View, Text, StyleSheet, Image, Pressable } from "react-native";
import { Swipeable } from "react-native-gesture-handler";
import { useEffect, useMemo, useRef } from "react";
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

function normalizeBookKey(name: string) {
  return name.toLowerCase().replace(/[\s_]/g, "");
}

function formatOdds(o: number) {
  return o > 0 ? `+${o}` : `${o}`;
}

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
  const hitPct = Math.round((hitRateL10 ?? 0) * 100);

  // ---------------------------
  // MULTI-BOOK NORMALIZATION
  // ---------------------------
  const resolvedBooks: BookOdds[] = useMemo(() => {
    if (books && books.length > 0) return books;
    if (bookmaker) return [{ bookmaker, odds }];
    return [];
  }, [books, bookmaker, odds]);

  // ---------------------------
  // CONFIDENCE TIER STYLING
  // ---------------------------
  const tier = useMemo(() => {
    if (confidence >= 80) return "elite";
    if (confidence >= 65) return "good";
    return "mid";
  }, [confidence]);

  const accentColor =
    tier === "elite"
      ? colors.success
      : tier === "good"
      ? colors.accent
      : "rgba(255,255,255,0.30)";

  const confidenceColor =
    tier === "elite"
      ? colors.success
      : tier === "good"
      ? colors.accent
      : colors.textSecondary;

  // ---------------------------
  // ODDS CHANGE ANIMATION
  // ---------------------------
  const prevOddsRef = useRef<Record<string, number>>({});
  const flash = useSharedValue(0);

  useEffect(() => {
    let changed = false;

    resolvedBooks.forEach(({ bookmaker, odds }) => {
      const key = normalizeBookKey(bookmaker);
      const prev = prevOddsRef.current[key];

      if (prev !== undefined && prev !== odds) changed = true;
      prevOddsRef.current[key] = odds;
    });

    if (changed) {
      flash.value = withTiming(1, { duration: 120 }, () => {
        flash.value = withTiming(0, { duration: 520 });
      });
    }
  }, [resolvedBooks]);

  const flashStyle = useAnimatedStyle(() => ({
    backgroundColor:
      flash.value > 0 ? "rgba(61,255,181,0.10)" : "rgba(0,0,0,0)",
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
      scale.value = withSpring(1.05, { damping: 12 });
      scale.value = withSpring(1, { damping: 14 });
    } else {
      opacity.value = withTiming(0.85, { duration: 110 }, () => {
        opacity.value = withTiming(1, { duration: 160 });
      });
    }
  }, [saved]);

  const handleToggleSave = () => {
    Haptics.impactAsync(
      saved ? Haptics.ImpactFeedbackStyle.Light : Haptics.ImpactFeedbackStyle.Medium
    );
    onToggleSave();
  };

  // ---------------------------
  // PRESS FEEDBACK (PRO)
  // ---------------------------
  const pressScale = useSharedValue(1);
  const pressAnimStyle = useAnimatedStyle(() => ({
    transform: [{ scale: pressScale.value }],
  }));

  const onPressIn = () => {
    pressScale.value = withSpring(0.985, { damping: 18 });
  };

  const onPressOut = () => {
    pressScale.value = withSpring(1, { damping: 18 });
  };

  return (
    <Swipeable overshootRight={false} onSwipeableOpen={handleToggleSave}>
      <Animated.View style={[animatedStyle, styles.outer]}>
        <Animated.View style={[styles.card, flashStyle]}>
          {/* LEFT ACCENT STRIP */}
          <View style={[styles.accentStrip, { backgroundColor: accentColor }]} />

          {/* TOP-RIGHT SAVE BUTTON */}
          <Pressable
            onPress={handleToggleSave}
            hitSlop={10}
            style={styles.saveButton}
          >
            <Text style={[styles.saveStar, saved ? styles.saveStarOn : styles.saveStarOff]}>
              {saved ? "★" : "☆"}
            </Text>
          </Pressable>

          {/* WHOLE CARD PRESS FEEL (no action, just feel “native”) */}
          <Pressable onPressIn={onPressIn} onPressOut={onPressOut}>
            <Animated.View style={pressAnimStyle}>
              {/* HEADER */}
              <View style={styles.headerRow}>
                {/* LEFT: TEAM LOGOS */}
                <View style={styles.teams}>
                  <View style={styles.teamStack}>
                    {away && TEAM_LOGOS[away] ? (
                      <Image source={{ uri: TEAM_LOGOS[away] }} style={styles.teamLogo} />
                    ) : (
                      <View style={styles.teamLogoPlaceholder} />
                    )}

                    {home && TEAM_LOGOS[home] ? (
                      <Image source={{ uri: TEAM_LOGOS[home] }} style={styles.teamLogo} />
                    ) : (
                      <View style={styles.teamLogoPlaceholder} />
                    )}
                  </View>
                </View>

                {/* CENTER: TEXT */}
                <View style={styles.center}>
                  <Text numberOfLines={1} style={styles.player}>
                    {player}
                  </Text>

                  <Text numberOfLines={1} style={styles.marketLine}>
                    {market} • {line}
                  </Text>

                  {matchup ? (
                    <Text numberOfLines={1} style={styles.matchup}>
                      {matchup}
                    </Text>
                  ) : (
                    <Text numberOfLines={1} style={styles.matchup}>
                      {" "}
                    </Text>
                  )}
                </View>

                {/* RIGHT: ODDS STACK */}
                <View style={styles.right}>
                  {resolvedBooks.length === 0 ? (
                    <View style={styles.oddsPill}>
                      <Text style={styles.oddsTextMuted}>—</Text>
                    </View>
                  ) : (
                    resolvedBooks.slice(0, 3).map((b) => {
                      const key = normalizeBookKey(b.bookmaker);

                      return (
                        <View key={key} style={styles.oddsPill}>
                          {BOOKMAKER_LOGOS[key] ? (
                            <Image
                              source={BOOKMAKER_LOGOS[key]}
                              style={styles.bookLogo}
                              resizeMode="contain"
                            />
                          ) : (
                            <View style={styles.bookLogoPlaceholder} />
                          )}

                          <Text style={styles.oddsText}>{formatOdds(b.odds)}</Text>
                        </View>
                      );
                    })
                  )}
                </View>
              </View>

              {/* DIVIDER */}
              <View style={styles.divider} />

              {/* METRICS ROW */}
              <View style={styles.metricsRow}>
                <View style={styles.metricLeft}>
                  <Text style={[styles.hitText, { color: confidenceColor }]}>
                    {hitPct}% HIT
                  </Text>
                  <Text style={styles.metricSub}>Last 10</Text>
                </View>

                <View style={styles.metricRight}>
                  <View style={styles.badge}>
                    <Text style={styles.badgeLabel}>CONF</Text>
                    <Text style={[styles.badgeValue, { color: confidenceColor }]}>
                      {confidence}
                    </Text>
                  </View>
                </View>
              </View>

              {/* CONFIDENCE BAR (TRACK + FILL) */}
              <View style={styles.barRow}>
                <View style={styles.barTrack}>
                  <View
                    style={[
                      styles.barFill,
                      {
                        width: `${Math.max(0, Math.min(100, confidence))}%`,
                        backgroundColor: confidenceColor,
                      },
                    ]}
                  />
                </View>
              </View>
            </Animated.View>
          </Pressable>
        </Animated.View>
      </Animated.View>
    </Swipeable>
  );
}

const styles = StyleSheet.create({
  // ---------------------------
  // CARD WRAPPER
  // ---------------------------
  outer: {
    marginHorizontal: 14,
    marginVertical: 10, // was 9
  },

  card: {
    backgroundColor: "#F8FAFC",
    borderRadius: 18,
    paddingVertical: 16, // was 14
    paddingHorizontal: 14,

    borderWidth: 1,
    borderColor: "#E5E7EB",

    shadowColor: "#0F172A", // was #000
    shadowOpacity: 0.06,    // was 0.08
    shadowRadius: 10,
    shadowOffset: { width: 0, height: 5 },
    elevation: 3,

    overflow: "hidden",
  },

  // ---------------------------
  // LEFT CONFIDENCE STRIP
  // ---------------------------
  accentStrip: {
    position: "absolute",
    left: 0,
    top: 0,
    bottom: 0,
    width: 4,
    borderTopLeftRadius: 18,
    borderBottomLeftRadius: 18,
  },

  // ---------------------------
  // SAVE BUTTON
  // ---------------------------
  saveButton: {
    position: "absolute",
    top: 10,
    right: 12,
    zIndex: 10,

    width: 34,
    height: 34,
    borderRadius: 12,
    alignItems: "center",
    justifyContent: "center",

    backgroundColor: "#FFFFFF", // was #F3F4F6
    borderWidth: 0,             // was 1
    borderColor: "#E5E7EB",

    shadowColor: "#000",
    shadowOpacity: 0.05,
    shadowRadius: 4,
    shadowOffset: { width: 0, height: 2 },
  },

  saveStar: {
    fontSize: 18,
    fontWeight: "900",
  },
  saveStarOn: {
    color: colors.accent,
  },
  saveStarOff: {
    color: colors.textSecondary,
  },

  // ---------------------------
  // HEADER ROW
  // ---------------------------
  headerRow: {
    flexDirection: "row",
    alignItems: "center",
    gap: 10,
  },

  // ---------------------------
  // TEAM LOGOS
  // ---------------------------
  teams: {
    width: 56,
    alignItems: "flex-start",
    justifyContent: "center",
  },

  teamStack: {
    flexDirection: "row",
    gap: 6,
  },

  teamLogo: {
    width: 20, // was 22
    height: 20,
    borderRadius: 6,
    backgroundColor: "#FFFFFF", // was #F3F4F6

    borderWidth: 1,             // added
    borderColor: "#E5E7EB",     // added

    padding: 2,                 // added
  },

  teamLogoPlaceholder: {
    width: 20, // was 22
    height: 20,
    borderRadius: 6,
    backgroundColor: "#E5E7EB", // was #F3F4F6
    borderWidth: 1,
    borderColor: "#D1D5DB",     // was #E5E7EB
  },

  // ---------------------------
  // CENTER TEXT
  // ---------------------------
  center: {
    flex: 1,
    alignItems: "flex-start", // was center
    paddingHorizontal: 6,
  },

  player: {
    color: colors.textPrimary,
    fontSize: textStyles.title,
    fontWeight: "800",
    letterSpacing: 0.2,
  },

  marketLine: {
    color: colors.textSecondary,
    fontSize: textStyles.subtitle,
    fontWeight: "700",
    marginTop: 2,
    letterSpacing: 0.2,
  },

  matchup: {
    color: colors.textSecondary,
    fontSize: textStyles.label,
    marginTop: 2,
  },

  // ---------------------------
  // ODDS (RIGHT COLUMN)
  // ---------------------------
  right: {
    width: 120,      // was 108
    paddingTop: 36,  // added
    alignItems: "flex-end",
    justifyContent: "center",
    gap: 6,
  },

  oddsPill: {
    flexDirection: "row",
    alignItems: "center",
    gap: 6,

    paddingVertical: 5,
    paddingHorizontal: 8,
    borderRadius: 10,

    backgroundColor: "#F1F5F9", // was #F3F4F6
    borderWidth: 1,
    borderColor: "#E2E8F0",     // was #E5E7EB
  },

  bookLogo: {
    width: 16,
    height: 16,
  },

  bookLogoPlaceholder: {
    width: 16,
    height: 16,
    borderRadius: 4,
    backgroundColor: "#E5E7EB",
  },

  oddsText: {
    color: colors.textPrimary,
    fontSize: textStyles.label,
    fontWeight: "800",
    letterSpacing: 0.2,
  },

  oddsTextMuted: {
    color: colors.textSecondary,
    fontSize: textStyles.label,
    fontWeight: "800",
  },

  // ---------------------------
  // DIVIDER
  // ---------------------------
  divider: {
    height: 1,
    backgroundColor: "#E5E7EB", // was #D1D5DB
    marginTop: 10,              // was 12
    marginBottom: 8,            // was 10
  },

  // ---------------------------
  // METRICS ROW
  // ---------------------------
  metricsRow: {
    flexDirection: "row",
    alignItems: "center",
    justifyContent: "space-between",
  },

  metricLeft: {
    flexDirection: "column",
  },

  hitText: {
    fontSize: textStyles.stat,
    fontWeight: "900",
    letterSpacing: 0.3,
  },

  metricSub: {
    color: colors.textSecondary,
    fontSize: 12,
    marginTop: 2,
    fontWeight: "700",
  },

  metricRight: {
    alignItems: "flex-end",
  },

  // ---------------------------
  // CONFIDENCE BADGE
  // ---------------------------
  badge: {
    flexDirection: "row",
    alignItems: "baseline",
    gap: 6,

    paddingVertical: 4,   // was 6
    paddingHorizontal: 8, // was 10
    borderRadius: 10,

    backgroundColor: "#FFFFFF",
    borderWidth: 1,
    borderColor: "#E5E7EB",
  },

  badgeLabel: {
    color: colors.textSecondary,
    fontSize: 9,          // was 10
    fontWeight: "900",
    letterSpacing: 0.8,
  },

  badgeValue: {
    fontSize: 15,         // was 14
    fontWeight: "900",
    letterSpacing: 0.2,
  },

  // ---------------------------
  // CONFIDENCE BAR
  // ---------------------------
  barRow: {
    marginTop: 10,
  },

  barTrack: {
    height: 6,                     // was 8
    borderRadius: 999,
    backgroundColor: "#D1FAE5",    // was #E5E7EB
    overflow: "hidden",
  },

  barFill: {
    height: "100%",
    borderRadius: 999,
  },
});