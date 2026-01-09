// components/PropCard.tsx
import { View, Text, StyleSheet, Image, Pressable } from "react-native";
import { Swipeable } from "react-native-gesture-handler";
import { useEffect, useMemo, useRef, useState } from "react";
import * as Haptics from "expo-haptics";
import Animated, {
  useSharedValue,
  useAnimatedStyle,
  withSpring,
  withTiming,
} from "react-native-reanimated";

import { useTheme } from "@/store/useTheme";
import textStyles from "../theme/text";
import { BOOKMAKER_LOGOS } from "../utils/bookmakerLogos";
import { Sparkline } from "./Sparkline";
import { BarSparkline } from "./BarSparkline";

/* ======================================================
   TEAM LOGOS
====================================================== */
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

/* ======================================================
   TYPES
====================================================== */
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

  avg_l5?: number;
  avg_l10?: number;
  avg_l20?: number;

  hit_rate_l5?: number;
  hit_rate_l10?: number;
  hit_rate_l20?: number;

  clear_1p_pct_l5?: number;
  clear_1p_pct_l10?: number;
  clear_1p_pct_l20?: number;

  clear_2p_pct_l5?: number;
  clear_2p_pct_l10?: number;
  clear_2p_pct_l20?: number;

  avg_margin_l5?: number;
  avg_margin_l10?: number;
  avg_margin_l20?: number;

  bad_miss_pct_l5?: number;
  bad_miss_pct_l10?: number;
  bad_miss_pct_l20?: number;

  pace_l5?: number;
  pace_l10?: number;
  pace_l20?: number;

  usage_l5?: number;
  usage_l10?: number;
  usage_l20?: number;

  ts_l10?: number;
  pace_delta?: number;
  delta_vs_line?: number;

  matchup?: string;
  home?: string;
  away?: string;
  bookmaker?: string;
  books?: BookOdds[];

  sparkline_l5?: number[];
  sparkline_l10?: number[];
  sparkline_l20?: number[];

  saved: boolean;
  onToggleSave: () => void;
  expanded: boolean;
  onToggleExpand: () => void;
};

/* ======================================================
   HELPERS
====================================================== */
function normalizeBookKey(name: string) {
  return name.toLowerCase().replace(/[\s_]/g, "");
}

function formatOdds(o: number) {
  return o > 0 ? `+${o}` : `${o}`;
}

/* ======================================================
   COMPONENT
====================================================== */
export default function PropCard(props: PropCardProps) {
  const colors = useTheme((s) => s.colors);

  const styles = useMemo(() => makeStyles(colors), [colors]);
  const {
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
    pace_delta,        // ✅ ADD THIS
    delta_vs_line,     // (you already use this too)
    ts_l10,
    saved,
    onToggleSave,
    expanded,
    onToggleExpand,
  } = props;

  const hitPct = Math.round(((props.hit_rate_l10 ?? 0) as number) * 100);
  
  /* =========================
     WINDOW TOGGLE
  ========================= */
  const [window, setWindow] = useState<5 | 10 | 20>(10);
  const w = window === 5 ? "l5" : window === 20 ? "l20" : "l10";

  const avg = w === "l5" ? props.avg_l5 : w === "l20" ? props.avg_l20 : props.avg_l10;
  const hitRate = w === "l5" ? props.hit_rate_l5 : w === "l20" ? props.hit_rate_l20 : props.hit_rate_l10;
  const clear1 = w === "l5" ? props.clear_1p_pct_l5 : w === "l20" ? props.clear_1p_pct_l20 : props.clear_1p_pct_l10;
  const clear2 = w === "l5" ? props.clear_2p_pct_l5 : w === "l20" ? props.clear_2p_pct_l20 : props.clear_2p_pct_l10;
  const margin = w === "l5" ? props.avg_margin_l5 : w === "l20" ? props.avg_margin_l20 : props.avg_margin_l10;
  const badMiss = w === "l5" ? props.bad_miss_pct_l5 : w === "l20" ? props.bad_miss_pct_l20 : props.bad_miss_pct_l10;
  const pace = w === "l5" ? props.pace_l5 : w === "l20" ? props.pace_l20 : props.pace_l10;
  const usage = w === "l5" ? props.usage_l5 : w === "l20" ? props.usage_l20 : props.usage_l10;
  const sparkline =
    w === "l5"
      ? props.sparkline_l5
      : w === "l20"
      ? props.sparkline_l20
      : props.sparkline_l10;

  /* =========================
     MULTI-BOOK NORMALIZATION
  ========================= */
  const resolvedBooks: BookOdds[] = useMemo(() => {
    if (books && books.length > 0) return books;
    if (bookmaker) return [{ bookmaker, odds }];
    return [];
  }, [books, bookmaker, odds]);

  /* =========================
     CONFIDENCE TIER
  ========================= */
  const tier = useMemo(() => {
    if (confidence >= 80) return "elite";
    if (confidence >= 65) return "good";
    return "mid";
  }, [confidence]);

  const accentColor =
    tier === "elite"
      ? colors.accent.success
      : tier === "good"
      ? colors.accent.warning
      : colors.border.strong;

  const confidenceColor =
    tier === "elite"
      ? colors.accent.success
      : tier === "good"
      ? colors.accent.warning
      : colors.text.muted;


  
  /* =========================
     ODDS FLASH
  ========================= */
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

  const flashStyle = useAnimatedStyle(() => {
    if (flash.value > 0) {
      return { backgroundColor: colors.glow.success };
    }
    return {};
  });

  /* =========================
     SWIPE SAVE
  ========================= */
  const swipeableRef = useRef<Swipeable>(null);

  const renderSaveAction = () => (
    <View
      style={{
        flex: 1,
        justifyContent: "center",
        paddingLeft: 24,
        backgroundColor: saved
          ? colors.surface.cardSoft
          : colors.glow.success,

      }}
    >
      <Text
        style={{
          fontSize: 16,
          fontWeight: "900",
          color: saved
            ? colors.text.secondary
            : colors.accent.success,

        }}
      >
        {saved ? "Unsave" : "Save"}
      </Text>
    </View>
  );

  const handleSwipeHaptic = () => {
    Haptics.impactAsync(
      saved
        ? Haptics.ImpactFeedbackStyle.Light
        : Haptics.ImpactFeedbackStyle.Medium
    );
  };

  const handleSwipeOpen = () => {
    onToggleSave();
    setTimeout(() => swipeableRef.current?.close(), 120);
  };

  /* =========================
     SAVE SCALE
  ========================= */
  const scale = useSharedValue(1);
  const animatedStyle = useAnimatedStyle(() => ({
    transform: [{ scale: scale.value }],
  }));

  useEffect(() => {
    if (saved) {
      scale.value = withSpring(1.05, { damping: 12 });
      scale.value = withSpring(1, { damping: 14 });
    }
  }, [saved]);

  /* =========================
     EXPAND ANIMATION
  ========================= */
  const expand = useSharedValue(0);
  const EXPANDED_HEIGHT = 420;

  useEffect(() => {
    expand.value = withSpring(expanded ? EXPANDED_HEIGHT : 0, {
      damping: 18,
      stiffness: 180,
    });
  }, [expanded]);

  const expandStyle = useAnimatedStyle(() => ({
    height: expand.value,
    opacity: expand.value === 0 ? 0 : 1,
  }));

  /* =========================
     PRESS FEEDBACK
  ========================= */
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

  /* =========================
     ROW COMPONENT
  ========================= */
  const ExpandRow = ({ label, value }: { label: string; value?: string }) => (
    <View style={styles.expandRow}>
      <Text style={styles.expandLabel}>{label}</Text>
      <Text style={styles.expandValue}>{value ?? "—"}</Text>
    </View>
  );

/* ======================================================
   RENDER
====================================================== */
return (
  <Swipeable
    ref={swipeableRef}
    overshootRight={false}
    renderLeftActions={renderSaveAction}
    leftThreshold={60}
    friction={2}
    onSwipeableWillOpen={handleSwipeHaptic}
    onSwipeableOpen={handleSwipeOpen}
  >
    <Animated.View style={[animatedStyle, styles.outer]}>
      <Animated.View style={[styles.card, flashStyle]}>
        {/* ACCENT STRIP */}
        <View
          style={[styles.accentStrip, { backgroundColor: accentColor }]}
        />

        {/* SAVE BUTTON */}
        <Pressable
          onPress={onToggleSave}
          hitSlop={10}
          style={styles.saveButton}
        >
          <Text
            style={[
              styles.saveStar,
              saved ? styles.saveStarOn : styles.saveStarOff,
            ]}
          >
            {saved ? "★" : "☆"}
          </Text>
        </Pressable>

        {/* MAIN CARD PRESS */}
        <Pressable
          onPress={onToggleExpand}
          onPressIn={onPressIn}
          onPressOut={onPressOut}
          hitSlop={4}
        >
          <Animated.View style={pressAnimStyle}>
            {/* HEADER */}
            <View style={styles.headerRow}>
              <View style={styles.teams}>
                <View style={styles.teamStack}>
                  {away && TEAM_LOGOS[away] ? (
                    <Image
                      source={{ uri: TEAM_LOGOS[away] }}
                      style={styles.teamLogo}
                    />
                  ) : (
                    <View style={styles.teamLogoPlaceholder} />
                  )}

                  {home && TEAM_LOGOS[home] ? (
                    <Image
                      source={{ uri: TEAM_LOGOS[home] }}
                      style={styles.teamLogo}
                    />
                  ) : (
                    <View style={styles.teamLogoPlaceholder} />
                  )}
                </View>
              </View>

              <View style={styles.center}>
                <Text numberOfLines={1} style={styles.player}>
                  {player}
                </Text>
                <Text numberOfLines={1} style={styles.marketLine}>
                  {market} • {line}
                </Text>
                <Text numberOfLines={1} style={styles.matchup}>
                  {matchup ?? " "}
                </Text>
              </View>

              <View style={styles.oddsTopRight}>
                {resolvedBooks.slice(0, 3).map((b) => {
                  const key = normalizeBookKey(b.bookmaker);
                  return (
                    <View key={key} style={styles.oddsPill}>
                      {BOOKMAKER_LOGOS[key] ? (
                        <Image
                          source={BOOKMAKER_LOGOS[key]}
                          style={styles.bookLogo}
                        />
                      ) : (
                        <View style={styles.bookLogoPlaceholder} />
                      )}
                      <Text style={styles.oddsText}>
                        {formatOdds(b.odds)}
                      </Text>
                    </View>
                  );
                })}
              </View>
            </View>

            {/* DIVIDER */}
            <View style={styles.divider} />

            {/* METRICS */}
            <View style={styles.metricsRow}>
              <View>
                <Text style={styles.hitText}>{hitPct}% HIT</Text>
                <Text style={styles.metricSub}>Last 10</Text>
              </View>

              <View style={styles.badge}>
                <Text style={styles.badgeLabel}>CONF</Text>
                <Text
                  style={[
                    styles.badgeValue,
                    { color: confidenceColor },
                  ]}
                >
                  {confidence}
                </Text>
              </View>
            </View>

            {/* CONFIDENCE BAR */}
            <View style={styles.confidenceRow}>
              <View style={styles.confidenceSpacer} />
              <View style={styles.confidenceBarWrap}>
                <View style={styles.confidenceBarTrack}>
                  <View
                    style={[
                      styles.confidenceBarFill,
                      {
                        width: `${confidence}%`,
                        backgroundColor: confidenceColor,
                      },
                    ]}
                  />
                </View>
              </View>
            </View>

            {/* EXPANDED SECTION (AUTO HEIGHT) */}
            {expanded && (
              <View style={styles.expandWrap}>
                <View style={styles.expandInner}>
                  {/* PERFORMANCE */}
                  <View style={styles.sectionHeader}>
                    <Text style={styles.sectionIcon}>📊</Text>
                    <Text style={styles.sectionText}>Performance</Text>
                  </View>

                  {/* 👇 BAR CHART ROW */}
                  <View style={{ alignItems: "center" }}>
                    <BarSparkline data={sparkline} height={42} />
                  </View>

                  <View style={styles.gridRow}>
                    <Text style={styles.statLabel}>AVG</Text>
                    <Text style={styles.statLabel}>HIT%</Text>
                    <Text style={styles.statLabel}>BAD MISS</Text>
                    <Text style={styles.statLabel}>PACE</Text>
                  </View>

                  <View style={styles.gridRow}>
                    <Text style={styles.statValue}>
                      {avg != null ? avg.toFixed(1) : "—"}
                    </Text>
                    <Text style={styles.statValue}>
                      {Math.round((hitRate ?? 0) * 100)}%
                    </Text>
                    <Text style={styles.statValue}>
                      {Math.round((badMiss ?? 0) * 100)}%
                    </Text>
                    <Text style={styles.statValue}>
                      {pace?.toFixed(1) ?? "—"}
                    </Text>
                  </View>

                  {/* EDGE */}
                  <View style={styles.sectionHeader}>
                    <Text style={styles.sectionIcon}>🎯</Text>
                    <Text style={styles.sectionText}>Edge</Text>
                  </View>

                  <View style={styles.gridRow}>
                    <Text style={styles.statLabel}>+1</Text>
                    <Text style={styles.statLabel}>+2</Text>
                    <Text style={styles.statLabel}>MARGIN</Text>
                    <Text style={styles.statLabel}>Δ LINE</Text>
                  </View>

                  <View style={styles.gridRow}>
                    <Text style={styles.statValue}>
                      {Math.round((clear1 ?? 0) * 100)}%
                    </Text>
                    <Text style={styles.statValue}>
                      {Math.round((clear2 ?? 0) * 100)}%
                    </Text>
                    <Text style={styles.statValue}>
                      {margin?.toFixed(1) ?? "—"}
                    </Text>
                    <Text style={styles.statValue}>
                      {props.delta_vs_line?.toFixed(1) ?? "—"}
                    </Text>
                  </View>

                  {/* CONTEXT */}
                  <View style={styles.sectionHeader}>
                    <Text style={styles.sectionIcon}>⚡</Text>
                    <Text style={styles.sectionText}>Context</Text>
                  </View>

                  <View style={styles.gridRow}>
                    <Text style={styles.statLabel}>TS%</Text>
                    <Text style={styles.statLabel}>USG%</Text>
                    <Text style={styles.statLabel}>PACE Δ</Text>
                  </View>

                  <View style={styles.gridRow}>
                    <Text style={styles.statValue}>
                      {props.ts_l10?.toFixed(3) ?? "—"}
                    </Text>
                    <Text style={styles.statValue}>
                      {Math.round((usage ?? 0) * 100)}%
                    </Text>
                    <Text style={styles.statValue}>
                      {pace_delta != null
                        ? pace_delta.toFixed(1)
                        : "—"}
                    </Text>
                  </View>
                </View>

                {/* WINDOW TOGGLE */}
                <View style={styles.windowToggle}>
                  <View style={styles.windowPillGroup}>
                    {[5, 10, 20].map((n) => {
                      const active = window === n;
                      return (
                        <Pressable
                          key={n}
                          onPress={() =>
                            setWindow(n as 5 | 10 | 20)
                          }
                          style={[
                            styles.windowPill,
                            active && styles.windowPillActive,
                          ]}
                        >
                          <Text
                            style={[
                              styles.windowPillLabel,
                              active &&
                                styles.windowPillLabelActive,
                            ]}
                          >
                            L{n}
                          </Text>
                        </Pressable>
                      );
                    })}
                  </View>
                </View>
              </View>
            )}
          </Animated.View>
        </Pressable>
      </Animated.View>
    </Animated.View>
  </Swipeable>
);
}
/* ======================================================
   STYLES
====================================================== */
function makeStyles(colors: any) {
  return StyleSheet.create({
    outer: {
      marginHorizontal: 14,
      marginVertical: 10,
    },

    card: {
      backgroundColor: colors.surface.card,
      borderRadius: 18,
      paddingVertical: 16,
      paddingHorizontal: 14,
      borderWidth: 1,
      borderColor: colors.border.subtle,
      shadowColor: "#000",
      shadowOpacity: 0.25,
      shadowRadius: 10,
      shadowOffset: { width: 0, height: 6 },
      elevation: 4,
      overflow: "hidden",
    },

    accentStrip: {
      position: "absolute",
      left: 0,
      top: 0,
      bottom: 0,
      width: 4,
    },

    saveButton: {
      position: "absolute",
      top: 10,
      right: 12,
      width: 34,
      height: 34,
      borderRadius: 12,
      alignItems: "center",
      justifyContent: "center",
      backgroundColor: colors.surface.elevated,
    },

    saveStar: {
      fontSize: 18,
      fontWeight: "900",
    },

    saveStarOn: { color: colors.accent.primary },
    saveStarOff: { color: colors.text.muted },

    headerRow: {
      flexDirection: "row",
      alignItems: "center",
      gap: 10,
    },

    teamLogo: {
      width: 18,
      height: 18,
      resizeMode: "contain",
    },

    teamLogoPlaceholder: {
      width: 18,
      height: 18,
      borderRadius: 4,
      backgroundColor: colors.surface.cardSoft,
    },


    player: {
      fontWeight: "800",
      color: colors.text.primary,
    },

    marketLine: {
      fontWeight: "700",
      color: colors.text.secondary,
    },

    matchup: {
      color: colors.text.muted,
    },

    oddsPill: {
      flexDirection: "row",
      alignItems: "center",
      gap: 6,
      paddingHorizontal: 8,
      paddingVertical: 4,
      borderRadius: 999,
      backgroundColor: colors.surface.cardSoft,
    },


    oddsText: {
      fontWeight: "800",
      color: colors.text.primary,
    },

    divider: {
      height: 1,
      backgroundColor: colors.border.subtle,
      marginVertical: 8,
    },

    hitText: {
      fontWeight: "900",
      color: colors.text.primary,
      letterSpacing: 0.3,
    },


    metricSub: {
      color: colors.text.muted,
    },

    badge: {
      flexDirection: "row",
      alignItems: "center",
      gap: 6,
      paddingHorizontal: 8,
      paddingVertical: 3,
      borderRadius: 999,
      borderWidth: 1,
      borderColor: colors.border.subtle,
},


    badgeLabel: {
      fontSize: 9,
      fontWeight: "900",
      color: colors.text.muted,
    },

    badgeValue: {
      fontSize: 15,
      fontWeight: "900",
    },

    expandWrap: { overflow: "hidden" },

    expandInner: {
      marginTop: 12,
      paddingTop: 12,
      borderTopWidth: 1,
      borderTopColor: colors.border.subtle,
    },

    sectionHeader: {
      flexDirection: "row",
      alignItems: "center",
      gap: 8,
      marginTop: 14,
      marginBottom: 6,
    },

    sectionText: {
      fontWeight: "900",
      letterSpacing: 0.6,
      textTransform: "uppercase",
      color: colors.text.secondary,
    },
    sectionIcon: {
      fontSize: 13,
      lineHeight: 13,
      opacity: 0.85,
    },
    expandedContainer: {
      gap: 12,
    },
    
    gridRow: {
      flexDirection: "row",
      justifyContent: "space-between",
    },
    
    statLabel: {
      flex: 1,
      textAlign: "center",
      fontSize: 11,
      fontWeight: "700",
      letterSpacing: 0.4,
      color: colors.text.muted,
    },
    
    statValue: {
      flex: 1,
      textAlign: "center",
      fontSize: 14,
      fontWeight: "900",
      color: colors.text.primary,
    },
    confidenceRow: {
      flexDirection: "row",
      alignItems: "center",
      marginTop: 6,
    },
    
    confidenceSpacer: {
      flex: 3, // 👈 left 75% empty
    },
    
    confidenceBarWrap: {
      flex: 1, // 👈 right 25%
      alignItems: "flex-end",
    },
    
    confidenceBarTrack: {
      width: "100%",
      height: 4,
      borderRadius: 999,
      backgroundColor: colors.surface.cardSoft,
      opacity: 0.6, // quieter
    },
    
    confidenceBarFill: {
      height: "100%",
      borderRadius: 999,
    },
    windowToggle: {
      marginTop: 14,
      alignItems: "center",
    },
    
    windowPillGroup: {
      flexDirection: "row",
      backgroundColor: colors.surface.cardSoft,
      borderRadius: 999,
      padding: 4,
      gap: 6,
    },
    
    windowPill: {
      paddingHorizontal: 14,
      paddingVertical: 6,
      borderRadius: 999,
      backgroundColor: "transparent",
    },
    
    windowPillActive: {
      backgroundColor: colors.surface.card,
      shadowColor: "#000",
      shadowOpacity: 0.18,
      shadowRadius: 6,
      shadowOffset: { width: 0, height: 3 },
      elevation: 3,
    },
    
    windowPillLabel: {
      fontSize: 12,
      fontWeight: "800",
      color: colors.text.muted,
      letterSpacing: 0.4,
    },
    
    windowPillLabelActive: {
      color: colors.text.primary,
    },
    bookLogo: {
      width: 14,
      height: 14,
      resizeMode: "contain",
    },

    bookLogoPlaceholder: {
      width: 14,
      height: 14,
      borderRadius: 3,
      backgroundColor: colors.surface.cardSoft,
    },
    metricsRow: {
      flexDirection: "row",
      alignItems: "flex-end",
      justifyContent: "space-between",
      marginTop: 6,
    },
    oddsTopRight: {
      position: "absolute",
      top: 12,
      right: 46, // 👈 pushes left so it doesn't collide with save star
      flexDirection: "row",
      gap: 6,
    },

  });
}