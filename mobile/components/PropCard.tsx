// components/PropCard.tsx
import { View, Text, StyleSheet, Image, Pressable, FlatList } from "react-native";
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
import { BOOKMAKER_LOGOS } from "@/utils/bookmakerLogos";
import { MiniBarSparkline } from "@/components/sparkline/MiniBarSparkline";
import { formatMarketLabel } from "@/utils/formatMarket";
import { STAT_META } from "@/lib/stats";
import { usePropBetslip } from "@/store/usePropBetslip";

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

export type PropCardProps = {
  player: string;
  market: string;
  side?: "over" | "under";
  line: number;
  odds: number;

  confidence: number;

  avg_l5?: number;
  avg_l10?: number;
  avg_l20?: number;

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
  playerImageUrl?: string;

  sparkline_l5?: number[];
  sparkline_l10?: number[];
  sparkline_l20?: number[];

  last5_dates?: string[];
  last10_dates?: string[];
  last20_dates?: string[];

  /** 🔑 FINAL HIT RATE — already side + window aware */
  hitRate: number;     // 0–1
  hitRatePct: number; // 0–100
  window?: "L5" | "L10" | "L20";

  saved: boolean;
  onToggleSave: () => void;
  expanded: boolean;
  onToggleExpand: () => void;
  scrollRef?: React.RefObject<FlatList<any>>;
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

function formatSideLabel(side?: "over" | "under") {
  return side === "under" ? "Under" : "Over";
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
    side,
    line,
    odds,
    confidence,
    matchup,
    home,
    away,
    bookmaker,
    books,
    pace_delta,
    delta_vs_line,
    ts_l10,
    saved,
    onToggleSave,
    expanded,
    onToggleExpand,
    scrollRef,
    hitRate,
    hitRatePct,
  } = props;

  /* =========================
     VISUAL WINDOW (DISPLAY ONLY)
  ========================= */
  const [window, setWindow] = useState<"L5" | "L10" | "L20">(props.window ?? "L10");
  const w = window === "L5" ? "l5" : window === "L20" ? "l20" : "l10";

  const avg =
    w === "l5" ? props.avg_l5 :
    w === "l20" ? props.avg_l20 :
    props.avg_l10;

  const clear1 =
    w === "l5" ? props.clear_1p_pct_l5 :
    w === "l20" ? props.clear_1p_pct_l20 :
    props.clear_1p_pct_l10;

  const clear2 =
    w === "l5" ? props.clear_2p_pct_l5 :
    w === "l20" ? props.clear_2p_pct_l20 :
    props.clear_2p_pct_l10;

  const margin =
    w === "l5" ? props.avg_margin_l5 :
    w === "l20" ? props.avg_margin_l20 :
    props.avg_margin_l10;

  const badMiss =
    w === "l5" ? props.bad_miss_pct_l5 :
    w === "l20" ? props.bad_miss_pct_l20 :
    props.bad_miss_pct_l10;

  const pace =
    w === "l5" ? props.pace_l5 :
    w === "l20" ? props.pace_l20 :
    props.pace_l10;

  const usage =
    w === "l5" ? props.usage_l5 :
    w === "l20" ? props.usage_l20 :
    props.usage_l10;

  const sparkline =
    w === "l5" ? props.sparkline_l5 :
    w === "l20" ? props.sparkline_l20 :
    props.sparkline_l10;

  const dates =
    w === "l5" ? props.last5_dates :
    w === "l20" ? props.last20_dates :
    props.last10_dates;

  /* =========================
     BOOKS
  ========================= */
  const resolvedBooks = useMemo<BookOdds[]>(() => {
    if (books?.length) return books;
    if (bookmaker) return [{ bookmaker, odds }];
    return [];
  }, [books, bookmaker, odds]);

  const uniqueBooks = useMemo(() => {
    const seen = new Map<string, BookOdds>();
    resolvedBooks.forEach((b) => {
      const k = `${normalizeBookKey(b.bookmaker)}-${b.odds}`;
      if (!seen.has(k)) seen.set(k, b);
    });
    return Array.from(seen.values());
  }, [resolvedBooks]);

  /* =========================
     CONFIDENCE COLOR
  ========================= */
  const tier =
    confidence >= 80 ? "elite" :
    confidence >= 65 ? "good" :
    "mid";

  const confidenceColor =
    tier === "elite"
      ? colors.accent.success
      : tier === "good"
      ? colors.accent.warning
      : colors.text.muted;

  /* =========================
     RENDER
  ========================= */
  return (
    <Swipeable
      overshootRight={false}
      renderLeftActions={() => null}
      simultaneousHandlers={scrollRef}
    >
      <View style={styles.outer}>
        <View style={styles.card}>
          {/* SAVE */}
          <Pressable onPress={onToggleSave} style={styles.saveButton}>
            <Text style={[styles.saveStar, saved && styles.saveStarOn]}>
              {saved ? "★" : "☆"}
            </Text>
          </Pressable>

          {/* HEADER */}
          <Pressable onPress={onToggleExpand}>
            <Text style={styles.player}>{player}</Text>
            <Text style={styles.marketLine}>
              {formatMarketLabel(market)} • {formatSideLabel(side)} {line}
            </Text>

            <Text style={styles.hitText}>{hitRatePct}% HIT</Text>
            <Text style={styles.metricSub}>Last {window}</Text>
          </Pressable>

          {/* EXPANDED */}
          {expanded && (
            <View style={styles.expandWrap}>
              <MiniBarSparkline data={sparkline} dates={dates} />

              <View style={styles.gridRow}>
                <Text style={styles.statValue}>{avg?.toFixed(1) ?? "—"}</Text>
                <Text style={styles.statValue}>{hitRatePct}%</Text>
                <Text style={styles.statValue}>{Math.round((badMiss ?? 0) * 100)}%</Text>
                <Text style={styles.statValue}>{pace?.toFixed(1) ?? "—"}</Text>
              </View>

              <View style={styles.windowToggle}>
                {(["L5", "L10", "L20"] as const).map((n) => (
                  <Pressable
                    key={n}
                    onPress={() => setWindow(n)}
                    style={[
                      styles.windowPill,
                      window === n && styles.windowPillActive,
                    ]}
                  >
                    <Text>{n}</Text>
                  </Pressable>
                ))}
              </View>
            </View>
          )}
        </View>
      </View>
    </Swipeable>
  );
}

/* ======================================================
   STYLES
====================================================== */
function makeStyles(colors: any) {
  return StyleSheet.create({
    outer: { margin: 12 },
    card: {
      backgroundColor: colors.surface.card,
      borderRadius: 16,
      padding: 14,
      borderWidth: 1,
      borderColor: colors.border.subtle,
    },
    saveButton: { position: "absolute", top: 8, right: 8 },
    saveStar: { fontSize: 18, color: colors.text.muted },
    saveStarOn: { color: colors.accent.primary },

    player: { fontWeight: "800", color: colors.text.primary },
    marketLine: { color: colors.text.secondary },
    hitText: { fontWeight: "900", marginTop: 6 },
    metricSub: { color: colors.text.muted },

    expandWrap: { marginTop: 12 },
    gridRow: { flexDirection: "row", justifyContent: "space-between" },
    statValue: { flex: 1, textAlign: "center", fontWeight: "800" },

    windowToggle: {
      flexDirection: "row",
      justifyContent: "center",
      marginTop: 10,
      gap: 8,
    },
    windowPill: {
      paddingHorizontal: 12,
      paddingVertical: 6,
      borderRadius: 999,
      borderWidth: 1,
      borderColor: colors.border.subtle,
    },
    windowPillActive: {
      backgroundColor: colors.surface.cardSoft,
    },
  });
}