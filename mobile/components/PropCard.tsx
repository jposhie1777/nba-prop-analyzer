// components/PropCard.tsx
import {
  View,
  Text,
  StyleSheet,
  Image,
  Pressable,
  FlatList,
} from "react-native";
import { Swipeable } from "react-native-gesture-handler";
import { useMemo, useState, useRef } from "react";

import { useTheme } from "@/store/useTheme";
import { BOOKMAKER_LOGOS } from "@/utils/bookmakerLogos";
import { MiniBarSparkline } from "@/components/sparkline/MiniBarSparkline";
import { formatMarketLabel } from "@/utils/formatMarket";
import { TEAM_LOGOS } from "@/utils/teamLogos";


/* ======================================================
   TYPES
====================================================== */
type BookOdds = {
  bookmaker: string;
  odds: number;
};

export type PropCardProps = {
  player: string;
  playerId?: number;
  playerImageUrl?: string;
  badLineScore?: number;

  market: string;
  side?: "over" | "under" | "yes";
  line: number;
  odds: number;

  bookmaker?: string;   // ✅ single source of truth
  books?: string;

  homeTeam?: string;
  awayTeam?: string;

  sparkline_l5?: number[];
  sparkline_l10?: number[];
  sparkline_l20?: number[];

  last5_dates?: string[];
  last10_dates?: string[];
  last20_dates?: string[];

  hitRate: number;
  hitRatePct: number;
  window?: "L5" | "L10" | "L20";

  avg_l5?: number;
  avg_l10?: number;
  avg_l20?: number;

  saved: boolean;
  onToggleSave: () => void;
  onSwipeSave?: () => void;

  expanded: boolean;
  onToggleExpand: () => void;
  scrollRef?: React.RefObject<FlatList<any>>;
};


/* ======================================================
   HELPERS
====================================================== */
function resolveBookmakerKey(raw?: string) {
  if (!raw) return null;

  const key = raw.toLowerCase().replace(/[\s_]/g, "");

  if (key.startsWith("draft")) return "draftkings";
  if (key.startsWith("fan")) return "fanduel";
  if (key === "dk") return "draftkings";
  if (key === "fd") return "fanduel";

  return key;
}



function formatOdds(o?: number) {
  if (o == null) return "";
  return o > 0 ? `+${o}` : `${o}`;
}

function formatSideLabel(side?: "over" | "under" | "yes") {
  if (side === "under") return "Under";
  if (side === "yes") return "Yes";
  return "Over";
}

/* ======================================================
   COMPONENT
====================================================== */
export default function PropCard(props: PropCardProps) {
  const colors = useTheme((s) => s.colors);
  const styles = useMemo(() => makeStyles(colors), [colors]);
  const swipeRef = useRef<Swipeable>(null);
  const {
    player,
    playerId,
    playerImageUrl,
    market,
    side,
    line,
    odds,
    bookmaker,
    books,
    saved,
    onToggleSave,
    onSwipeSave,
    expanded,
    onToggleExpand,
    scrollRef,
    hitRatePct,
  } = props;

  /* =========================
     DISPLAY WINDOW
  ========================= */
  const [displayWindow, setDisplayWindow] = useState<"L5" | "L10" | "L20">(
    props.window ?? "L10"
  );

  const hitRate =
    displayWindow === "L5"
      ? props.hitRatePct
      : displayWindow === "L20"
      ? props.hitRatePct
      : props.hitRatePct;

  const windowAvg =
    displayWindow === "L5"
      ? props.avg_l5
      : displayWindow === "L20"
      ? props.avg_l20
      : props.avg_l10;

  const sparkline =
    displayWindow === "L5"
      ? props.sparkline_l5
      : displayWindow === "L20"
      ? props.sparkline_l20
      : props.sparkline_l10;

  const dates =
    displayWindow === "L5"
      ? props.last5_dates
      : displayWindow === "L20"
      ? props.last20_dates
      : props.last10_dates;

  /* =========================
     IMAGE
  ========================= */
  const imageUrl =
    playerImageUrl ||
    (playerId
      ? `https://a.espncdn.com/i/headshots/nba/players/full/${playerId}.png`
      : null);

  const homeLogo = props.homeTeam
  ? TEAM_LOGOS[props.homeTeam]
  : null;

  const awayLogo = props.awayTeam
    ? TEAM_LOGOS[props.awayTeam]
    : null;
    
  /* =========================
     BOOK
  ========================= */
  const resolvedBook = useMemo(() => {
  if (bookmaker) {
    return { bookmaker, odds };
  }
  return null;
}, [bookmaker, odds]);

  const bookKey = resolveBookmakerKey(resolvedBook?.bookmaker);

  const bookLogo = bookKey ? BOOKMAKER_LOGOS[bookKey] : null;

    
  /* =========================
     SWIPE UI
  ========================= */
  const renderSwipeSave = () => (
    <View style={styles.swipeSave}>
      <Text style={styles.swipeSaveText}>SAVE</Text>
    </View>
  );

  /* =========================
     RENDER
  ========================= */
  return (
    <Swipeable
      ref={swipeRef}
      overshootRight={false}
      simultaneousHandlers={scrollRef}
      renderLeftActions={renderSwipeSave}
      onSwipeableLeftOpen={() => {
        if (!saved) {
          onSwipeSave?.();
        }
    
        // ✅ THIS IS THE CRITICAL LINE
        requestAnimationFrame(() => {
          swipeRef.current?.close();
        });
      }}
    >
      <View style={styles.outer}>
        <View style={styles.card}>
          {/* SAVE STAR */}
          <Pressable onPress={onToggleSave} style={styles.saveButton}>
            <Text style={[styles.saveStar, saved && styles.saveStarOn]}>
              {saved ? "★" : "☆"}
            </Text>
          </Pressable>

          {/* TOP HEADER SECTION */}
          <Pressable onPress={onToggleExpand} style={styles.header}>
            {/* LEFT – MATCHUP */}
            <View style={styles.headerLeft}>
              {awayLogo && <Image source={{ uri: awayLogo }} style={styles.teamLogo} />}
              <Text style={styles.atSymbol}>@</Text>
              {homeLogo && <Image source={{ uri: homeLogo }} style={styles.teamLogo} />}
            </View>

            {/* CENTER – PLAYER + MARKET */}
            <View style={styles.headerCenter}>
              {imageUrl && (
                <Image source={{ uri: imageUrl }} style={styles.headshot} />
              )}
            
              <View style={styles.headerTextBlock}>
                <Text style={styles.player}>{player}</Text>
            
                <Text style={styles.marketLine}>
                  {formatMarketLabel(market)} · {formatSideLabel(side)} {line}
                </Text>
            
                {/* 🚨 BAD LINE BADGE */}
                {badLineScore != null && badLineScore >= 1.0 && (
                  <Pressable style={styles.badLineBadge}>
                    <Text style={styles.badLineText}>⚠️ BAD LINE</Text>
                  </Pressable>
                )}
              </View>
            </View>


            {/* RIGHT – BOOK + ODDS */}
            <View style={styles.headerRight}>
              <View style={styles.bookOddsRow}>

                {/* 🔵 REAL LOGO */}
                <Image
                  source={bookLogo}
                  style={styles.bookLogo}
                  resizeMode="contain"
                />


                <Text style={styles.oddsText}>
                  {formatOdds(resolvedBook?.odds ?? odds)}
                </Text>
              </View>
            </View>

          </Pressable>
          <View style={styles.bottomRow}>
            <View style={styles.metricBox}>
              <Text style={styles.metricLabel}>{displayWindow} HIT</Text>
              <Text style={styles.metricValue}>
                {hitRate != null ? `${hitRate}%` : "—"}
              </Text>
            </View>

            <View style={styles.metricBox}>
              <Text style={styles.metricLabel}>{displayWindow} AVG</Text>
              <Text style={styles.metricValue}>
                {windowAvg != null ? windowAvg.toFixed(1) : "—"}
              </Text>
            </View>
          </View>


          {/* EXPANDED */}
          {expanded && (
            <View style={styles.expandWrap}>
              <MiniBarSparkline data={sparkline} dates={dates} />

              <View style={styles.windowToggle}>
                {(["L5", "L10", "L20"] as const).map((n) => (
                  <Pressable
                    key={n}
                    onPress={() => setDisplayWindow(n)}
                    style={[
                      styles.windowPill,
                      displayWindow === n && styles.windowPillActive,
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
    /* =========================
       WRAPPER
    ========================= */
    outer: {
      marginHorizontal: 12,
      marginVertical: 8,
    },

    card: {
      backgroundColor: colors.surface.card,
      borderRadius: 16,
      padding: 14,
      borderWidth: 1,
      borderColor: colors.border.subtle,
    },

    /* =========================
       SAVE STAR
    ========================= */
    saveButton: {
      position: "absolute",
      top: 8,
      right: 8,
      zIndex: 2,
    },

    saveStar: {
      fontSize: 18,
      color: colors.text.muted,
    },

    saveStarOn: {
      color: colors.accent.primary,
    },

    /* =========================
       TOP HEADER SECTION
    ========================= */
    header: {
      position: "relative",
      paddingBottom: 10,
      marginBottom: 8,
      borderBottomWidth: StyleSheet.hairlineWidth,
      borderBottomColor: colors.border.subtle,
    },

    headerLeft: {
      position: "absolute",
      left: 0,
      top: 0,
      flexDirection: "row",
      alignItems: "center",
      gap: 6,
    },

    headerRight: {
      position: "absolute",
      right: 0,
      top: 0,
      alignItems: "flex-end",
    },


    headerCenter: {
      flexDirection: "row",
      alignItems: "center",
      alignSelf: "center",
      gap: 10,
    },

    headerTextBlock: {
      alignItems: "flex-start",
    },


    /* =========================
       IMAGES
    ========================= */
    headshot: {
      width: 38,
      height: 38,
      borderRadius: 19,
      backgroundColor: colors.surface.cardSoft,
      marginBottom: 2,
    },


    teamLogo: {
      width: 22,
      height: 22,
      resizeMode: "contain",
    },

    bookLogo: {
      width: 28,
      height: 14,
      resizeMode: "contain",
      opacity: 0.95,
    },

    oddsText: {
      fontSize: 13,
      fontWeight: "800",
      color: colors.text.primary,
    },


    /* =========================
       TEXT
    ========================= */
    player: {
      fontSize: 15,
      fontWeight: "800",
      color: colors.text.primary,
      textAlign: "left",
    },

    marketLine: {
      fontSize: 12,
      color: colors.text.muted,
      marginTop: 2,
      textAlign: "left",
    },

    bookOddsRow: {
      flexDirection: "row",
      alignItems: "center",
      gap: 6,
    },

    atSymbol: {
      fontSize: 11,
      fontWeight: "700",
      color: colors.text.muted,
    },

    /* =========================
       BOTTOM METRICS ROW
    ========================= */
    bottomRow: {
      flexDirection: "row",
      justifyContent: "space-between",
      marginTop: 6,
    },

    metricBox: {
      alignItems: "flex-start",
    },

    metricLabel: {
      fontSize: 11,
      fontWeight: "700",
      color: colors.text.muted,
      marginBottom: 2,
    },

    metricValue: {
      fontSize: 16,
      fontWeight: "900",
      color: colors.accent.primary,
    },

    /* =========================
       EXPANDED AREA
    ========================= */
    expandWrap: {
      marginTop: 12,
    },

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
      borderColor: colors.surface.cardSoft,
    },

    /* =========================
       SWIPE SAVE
    ========================= */
    swipeSave: {
      flex: 1,
      justifyContent: "center",
      paddingLeft: 24,
      backgroundColor: colors.accent.primary,
      borderRadius: 16,
    },

    swipeSaveText: {
      color: colors.text.inverse,
      fontWeight: "900",
      fontSize: 14,
      letterSpacing: 0.5,
    },
    badLineBadge: {
      backgroundColor: "#2a1414",
      borderColor: "#ff4d4f",
      borderWidth: 1,
      paddingHorizontal: 8,
      paddingVertical: 3,
      borderRadius: 6,
      alignSelf: "flex-start",
      marginTop: 4,
    },
    badLineText: {
      color: "#ff4d4f",
      fontSize: 11,
      fontWeight: "600",
    },
  });
}
