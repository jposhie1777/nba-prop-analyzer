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

import colors from "../theme/color";
import textStyles from "../theme/text";
import { BOOKMAKER_LOGOS } from "../utils/bookmakerLogos";

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
    saved,
    onToggleSave,
    expanded,
    onToggleExpand,
  } = props;

  const hitPct = Math.round((hitRateL10 ?? 0) * 100);

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
      return { backgroundColor: "rgba(61,255,181,0.10)" };
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
        backgroundColor: saved ? "#E5E7EB" : "#D1FAE5",
      }}
    >
      <Text
        style={{
          fontSize: 16,
          fontWeight: "900",
          color: saved ? "#475569" : "#047857",
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
  ======================================================= */
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
          <View style={[styles.accentStrip, { backgroundColor: accentColor }]} />

          <Pressable onPress={onToggleSave} hitSlop={10} style={styles.saveButton}>
            <Text style={[styles.saveStar, saved ? styles.saveStarOn : styles.saveStarOff]}>
              {saved ? "★" : "☆"}
            </Text>
          </Pressable>

          <Pressable onPress={onToggleExpand} onPressIn={onPressIn} onPressOut={onPressOut} android_ripple={null} hitSlop={4}>
            <Animated.View style={pressAnimStyle}>
              {/* HEADER */}
              <View style={styles.headerRow}>
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

                <View style={styles.center}>
                  <Text numberOfLines={1} style={styles.player}>{player}</Text>
                  <Text numberOfLines={1} style={styles.marketLine}>{market} • {line}</Text>
                  <Text numberOfLines={1} style={styles.matchup}>{matchup ?? " "}</Text>
                </View>

                <View style={styles.right}>
                  {resolvedBooks.slice(0, 3).map((b) => {
                    const key = normalizeBookKey(b.bookmaker);
                    return (
                      <View key={key} style={styles.oddsPill}>
                        {BOOKMAKER_LOGOS[key] ? (
                          <Image source={BOOKMAKER_LOGOS[key]} style={styles.bookLogo} />
                        ) : (
                          <View style={styles.bookLogoPlaceholder} />
                        )}
                        <Text style={styles.oddsText}>{formatOdds(b.odds)}</Text>
                      </View>
                    );
                  })}
                </View>
              </View>

              <View style={styles.divider} />

              <View style={styles.metricsRow}>
                <View>
                  <Text style={[styles.hitText, { color: confidenceColor }]}>{hitPct}% HIT</Text>
                  <Text style={styles.metricSub}>Last 10</Text>
                </View>
                <View style={styles.badge}>
                  <Text style={styles.badgeLabel}>CONF</Text>
                  <Text style={[styles.badgeValue, { color: confidenceColor }]}>{confidence}</Text>
                </View>
              </View>

              <View style={styles.barRow}>
                <View style={styles.barTrack}>
                  <View style={[styles.barFill, { width: `${confidence}%`, backgroundColor: confidenceColor }]} />
                </View>
              </View>

              <Animated.View style={[styles.expandWrap, expandStyle]}>
                <View style={styles.expandInner}>
                  <Text style={styles.expandTitle}>Details</Text>

                  <ExpandRow label={`Avg (L${window})`} value={avg != null ? avg.toFixed(1) : "—"} />
                  <ExpandRow label="Hit Rate" value={`${Math.round((hitRate ?? 0) * 100)}%`} />
                  <ExpandRow label="Clear 1+" value={`${Math.round((clear1 ?? 0) * 100)}%`} />
                  <ExpandRow label="Clear 2+" value={`${Math.round((clear2 ?? 0) * 100)}%`} />
                  <ExpandRow label="Avg Margin" value={margin?.toFixed(1)} />
                  <ExpandRow label="Bad Miss" value={`${Math.round((badMiss ?? 0) * 100)}%`} />
                  <ExpandRow label="Pace" value={pace?.toFixed(1)} />
                  <ExpandRow label="Usage" value={`${Math.round((usage ?? 0) * 100)}%`} />

                  <View style={styles.expandDivider} />

                  <ExpandRow label="TS (L10)" value={props.ts_l10?.toFixed(3)} />
                  <ExpandRow label="Pace Δ" value={props.pace_delta?.toFixed(1)} />
                  <ExpandRow label="Δ vs Line" value={props.delta_vs_line?.toFixed(1)} />

                  <View style={styles.windowToggle}>
                    {[5, 10, 20].map((n) => (
                      <Pressable
                        key={n}
                        onPress={() => setWindow(n as 5 | 10 | 20)}
                        style={[
                          styles.windowBtn,
                          window === n && styles.windowBtnActive,
                        ]}
                      >
                        <Text
                          style={[
                            styles.windowLabel,
                            window === n && styles.windowLabelActive,
                          ]}
                        >
                          L{n}
                        </Text>
                      </Pressable>
                    ))}
                  </View>
                </View>
              </Animated.View>
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
const styles = StyleSheet.create({
  outer: { marginHorizontal: 14, marginVertical: 10 },
  card: {
    backgroundColor: "#F8FAFC",
    borderRadius: 18,
    paddingVertical: 16,
    paddingHorizontal: 14,
    borderWidth: 1,
    borderColor: "#E5E7EB",
    shadowColor: "#0F172A",
    shadowOpacity: 0.06,
    shadowRadius: 10,
    shadowOffset: { width: 0, height: 5 },
    elevation: 3,
    overflow: "hidden",
  },
  accentStrip: { position: "absolute", left: 0, top: 0, bottom: 0, width: 4 },
  saveButton: {
    position: "absolute",
    top: 10,
    right: 12,
    width: 34,
    height: 34,
    borderRadius: 12,
    alignItems: "center",
    justifyContent: "center",
    backgroundColor: "#FFFFFF",
  },
  saveStar: { fontSize: 18, fontWeight: "900" },
  saveStarOn: { color: colors.accent },
  saveStarOff: { color: colors.textSecondary },

  headerRow: { flexDirection: "row", alignItems: "center", gap: 10 },
  teams: { width: 56 },
  teamStack: { flexDirection: "row", gap: 6 },
  teamLogo: { width: 20, height: 20 },
  teamLogoPlaceholder: { width: 20, height: 20, backgroundColor: "#E5E7EB" },

  center: { flex: 1, paddingHorizontal: 6 },
  player: { fontSize: textStyles.title, fontWeight: "800" },
  marketLine: { fontSize: textStyles.subtitle, fontWeight: "700" },
  matchup: { fontSize: textStyles.label },

  right: { width: 120, alignItems: "flex-end", gap: 6 },
  oddsPill: { flexDirection: "row", gap: 6, padding: 6, borderRadius: 10, backgroundColor: "#E5E7EB" },
  bookLogo: { width: 16, height: 16 },
  bookLogoPlaceholder: { width: 16, height: 16, backgroundColor: "#CBD5E1" },
  oddsText: { fontSize: 12, fontWeight: "800" },

  divider: { height: 1, backgroundColor: "#E5E7EB", marginVertical: 8 },
  metricsRow: { flexDirection: "row", justifyContent: "space-between" },
  hitText: { fontSize: textStyles.stat, fontWeight: "900" },
  metricSub: { fontSize: 12, color: colors.textSecondary },

  badge: {
    flexDirection: "row",
    gap: 6,
    paddingHorizontal: 8,
    paddingVertical: 4,
    borderRadius: 10,
    borderWidth: 1,
    borderColor: "#E5E7EB",
  },
  badgeLabel: { fontSize: 9, fontWeight: "900", color: colors.textSecondary },
  badgeValue: { fontSize: 15, fontWeight: "900" },

  barRow: { marginTop: 10 },
  barTrack: { height: 6, borderRadius: 999, backgroundColor: "#D1FAE5" },
  barFill: { height: "100%", borderRadius: 999 },

  expandWrap: { overflow: "hidden" },
  expandInner: { marginTop: 12, paddingTop: 12, borderTopWidth: 1, borderTopColor: "#E5E7EB" },
  expandTitle: { fontSize: 12, fontWeight: "900", marginBottom: 6 },

  expandRow: { flexDirection: "row", justifyContent: "space-between", marginBottom: 6 },
  expandLabel: { fontSize: 12, color: "#475569" },
  expandValue: { fontSize: 12, fontWeight: "800" },
  expandDivider: { height: 1, backgroundColor: "#E5E7EB", marginVertical: 10 },

  windowToggle: { flexDirection: "row", marginTop: 10 },
  windowBtn: { flex: 1, marginHorizontal: 4, paddingVertical: 6, borderRadius: 8, backgroundColor: "#E5E7EB" },
  windowBtnActive: { backgroundColor: colors.accent },
  windowLabel: { fontSize: 12, fontWeight: "800", color: "#334155" },
  windowLabelActive: { color: "#FFFFFF" },
});
