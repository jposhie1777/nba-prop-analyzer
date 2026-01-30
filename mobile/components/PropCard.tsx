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
import { LinearGradient } from "expo-linear-gradient";

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

function getHitRateColor(pct: number): string {
  if (pct >= 80) return "#22c55e"; // green
  if (pct >= 60) return "#3b82f6"; // blue
  if (pct >= 40) return "#f59e0b"; // amber
  return "#ef4444"; // red
}

/* ======================================================
   COMPONENT
====================================================== */
export default function PropCard(props: PropCardProps) {
  const colors = useTheme((s) => s.colors);
  const styles = useMemo(() => makeStyles(colors), [colors]);
  const swipeRef = useRef<Swipeable>(null);
  const [showBadLineInfo, setShowBadLineInfo] = useState(false);
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
    badLineScore,
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
     HIT RATE COLOR
  ========================= */
  const hitRateColor = getHitRateColor(hitRate ?? 0);

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

        requestAnimationFrame(() => {
          swipeRef.current?.close();
        });
      }}
    >
      <View style={styles.outer}>
        <View style={styles.card}>
          {/* TOP ACCENT LINE */}
          <View style={[styles.accentLine, { backgroundColor: hitRateColor }]} />

          {/* MAIN CONTENT */}
          <Pressable onPress={onToggleExpand} style={styles.contentWrap}>
            {/* PLAYER ROW */}
            <View style={styles.playerRow}>
              {/* HEADSHOT WITH RING */}
              <View style={styles.headshotWrap}>
                {imageUrl ? (
                  <Image source={{ uri: imageUrl }} style={styles.headshot} />
                ) : (
                  <View style={styles.headshotPlaceholder} />
                )}
                <View style={[styles.headshotRing, { borderColor: hitRateColor }]} />
              </View>

              {/* PLAYER INFO */}
              <View style={styles.playerInfo}>
                <Text style={styles.player} numberOfLines={1}>{player}</Text>
                <View style={styles.matchupRow}>
                  {awayLogo && <Image source={{ uri: awayLogo }} style={styles.teamLogoSmall} />}
                  <Text style={styles.atSymbol}>@</Text>
                  {homeLogo && <Image source={{ uri: homeLogo }} style={styles.teamLogoSmall} />}
                </View>
              </View>

              {/* ODDS CHIP */}
              <View style={styles.oddsChip}>
                {bookLogo && (
                  <Image
                    source={bookLogo}
                    style={styles.bookLogoSmall}
                    resizeMode="contain"
                  />
                )}
                <Text style={styles.oddsText}>
                  {formatOdds(resolvedBook?.odds ?? odds)}
                </Text>
              </View>
            </View>

            {/* PROP LINE */}
            <View style={styles.propLineRow}>
              <View style={styles.propLineBadge}>
                <Text style={styles.propLineText}>
                  {formatMarketLabel(market)}
                </Text>
              </View>
              <View style={styles.propValueBadge}>
                <Text style={styles.propValueText}>
                  {formatSideLabel(side)} {line}
                </Text>
              </View>

              {/* BAD LINE BADGE */}
              {typeof badLineScore === "number" && badLineScore >= 1.0 && (
                <Pressable
                  style={styles.badLineBadge}
                  onPress={() => setShowBadLineInfo((v) => !v)}
                >
                  <Text style={styles.badLineText}>⚠️ BAD LINE</Text>
                </Pressable>
              )}
            </View>

            {/* BAD LINE TOOLTIP */}
            {showBadLineInfo && typeof badLineScore === "number" && (
              <View style={styles.badLineTooltip}>
                <Text style={styles.tooltipTitle}>Bad Line Detected</Text>
                <Text style={styles.tooltipText}>
                  This line is mispriced relative to recent performance and opponent context.
                </Text>
                <Text style={styles.tooltipMeta}>
                  Score: {badLineScore.toFixed(2)}
                </Text>
                <Pressable
                  style={styles.tooltipClose}
                  onPress={() => setShowBadLineInfo(false)}
                >
                  <Text style={styles.tooltipCloseText}>Dismiss</Text>
                </Pressable>
              </View>
            )}

            {/* STATS ROW */}
            <View style={styles.statsRow}>
              {/* HIT RATE WITH PROGRESS */}
              <View style={styles.statBlock}>
                <Text style={styles.statLabel}>{displayWindow} HIT RATE</Text>
                <View style={styles.progressWrap}>
                  <View style={styles.progressTrack}>
                    <View
                      style={[
                        styles.progressFill,
                        {
                          width: `${Math.min(hitRate ?? 0, 100)}%`,
                          backgroundColor: hitRateColor
                        }
                      ]}
                    />
                  </View>
                  <Text style={[styles.statValue, { color: hitRateColor }]}>
                    {hitRate != null ? `${hitRate}%` : "—"}
                  </Text>
                </View>
              </View>

              {/* AVG */}
              <View style={styles.statBlockRight}>
                <Text style={styles.statLabel}>{displayWindow} AVG</Text>
                <Text style={styles.avgValue}>
                  {windowAvg != null ? windowAvg.toFixed(1) : "—"}
                </Text>
              </View>
            </View>
          </Pressable>

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
                    <Text style={[
                      styles.windowPillText,
                      displayWindow === n && styles.windowPillTextActive,
                    ]}>{n}</Text>
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
      marginVertical: 6,
    },

    card: {
      backgroundColor: colors.surface.card,
      borderRadius: 20,
      overflow: "hidden",
      shadowColor: "#000",
      shadowOffset: { width: 0, height: 4 },
      shadowOpacity: 0.15,
      shadowRadius: 12,
      elevation: 6,
    },

    accentLine: {
      height: 3,
      width: "100%",
    },

    contentWrap: {
      padding: 16,
    },

    /* =========================
       PLAYER ROW
    ========================= */
    playerRow: {
      flexDirection: "row",
      alignItems: "center",
      marginBottom: 14,
    },

    headshotWrap: {
      position: "relative",
      width: 52,
      height: 52,
      marginRight: 12,
    },

    headshot: {
      width: 48,
      height: 48,
      borderRadius: 24,
      backgroundColor: colors.surface.cardSoft,
    },

    headshotPlaceholder: {
      width: 48,
      height: 48,
      borderRadius: 24,
      backgroundColor: colors.surface.cardSoft,
    },

    headshotRing: {
      position: "absolute",
      top: -2,
      left: -2,
      width: 52,
      height: 52,
      borderRadius: 26,
      borderWidth: 2,
    },

    playerInfo: {
      flex: 1,
    },

    player: {
      fontSize: 17,
      fontWeight: "700",
      color: colors.text.primary,
      letterSpacing: -0.3,
      marginBottom: 4,
    },

    matchupRow: {
      flexDirection: "row",
      alignItems: "center",
      gap: 4,
    },

    teamLogoSmall: {
      width: 18,
      height: 18,
      resizeMode: "contain",
    },

    atSymbol: {
      fontSize: 10,
      fontWeight: "600",
      color: colors.text.muted,
      marginHorizontal: 2,
    },

    oddsChip: {
      flexDirection: "row",
      alignItems: "center",
      backgroundColor: colors.surface.cardSoft,
      paddingHorizontal: 10,
      paddingVertical: 6,
      borderRadius: 10,
      gap: 6,
    },

    bookLogoSmall: {
      width: 20,
      height: 12,
    },

    oddsText: {
      fontSize: 14,
      fontWeight: "800",
      color: colors.text.primary,
    },

    /* =========================
       PROP LINE ROW
    ========================= */
    propLineRow: {
      flexDirection: "row",
      alignItems: "center",
      gap: 8,
      marginBottom: 16,
    },

    propLineBadge: {
      backgroundColor: `${colors.accent.primary}15`,
      paddingHorizontal: 10,
      paddingVertical: 5,
      borderRadius: 8,
    },

    propLineText: {
      fontSize: 12,
      fontWeight: "600",
      color: colors.accent.primary,
    },

    propValueBadge: {
      backgroundColor: colors.surface.cardSoft,
      paddingHorizontal: 10,
      paddingVertical: 5,
      borderRadius: 8,
    },

    propValueText: {
      fontSize: 12,
      fontWeight: "700",
      color: colors.text.primary,
    },

    /* =========================
       STATS ROW
    ========================= */
    statsRow: {
      flexDirection: "row",
      alignItems: "flex-end",
      justifyContent: "space-between",
    },

    statBlock: {
      flex: 1,
      marginRight: 16,
    },

    statBlockRight: {
      alignItems: "flex-end",
    },

    statLabel: {
      fontSize: 10,
      fontWeight: "600",
      color: colors.text.muted,
      textTransform: "uppercase",
      letterSpacing: 0.5,
      marginBottom: 6,
    },

    progressWrap: {
      flexDirection: "row",
      alignItems: "center",
      gap: 10,
    },

    progressTrack: {
      flex: 1,
      height: 6,
      backgroundColor: colors.surface.cardSoft,
      borderRadius: 3,
      overflow: "hidden",
    },

    progressFill: {
      height: "100%",
      borderRadius: 3,
    },

    statValue: {
      fontSize: 16,
      fontWeight: "800",
      minWidth: 48,
      textAlign: "right",
    },

    avgValue: {
      fontSize: 22,
      fontWeight: "800",
      color: colors.text.primary,
    },

    /* =========================
       EXPANDED AREA
    ========================= */
    expandWrap: {
      marginTop: 4,
      paddingTop: 16,
      paddingHorizontal: 16,
      paddingBottom: 16,
      borderTopWidth: 1,
      borderTopColor: colors.border.subtle,
    },

    windowToggle: {
      flexDirection: "row",
      justifyContent: "center",
      marginTop: 14,
      gap: 8,
    },

    windowPill: {
      paddingHorizontal: 16,
      paddingVertical: 8,
      borderRadius: 12,
      backgroundColor: colors.surface.cardSoft,
    },

    windowPillActive: {
      backgroundColor: colors.accent.primary,
    },

    windowPillText: {
      fontSize: 12,
      fontWeight: "600",
      color: colors.text.muted,
    },

    windowPillTextActive: {
      color: colors.text.inverse,
    },

    /* =========================
       SWIPE SAVE
    ========================= */
    swipeSave: {
      flex: 1,
      justifyContent: "center",
      paddingLeft: 24,
      backgroundColor: colors.accent.primary,
      borderRadius: 20,
      marginVertical: 6,
    },

    swipeSaveText: {
      color: colors.text.inverse,
      fontWeight: "800",
      fontSize: 14,
      letterSpacing: 0.5,
    },

    /* =========================
       BAD LINE
    ========================= */
    badLineBadge: {
      backgroundColor: "#2a1414",
      borderColor: "#ff4d4f",
      borderWidth: 1,
      paddingHorizontal: 8,
      paddingVertical: 4,
      borderRadius: 8,
    },

    badLineText: {
      color: "#ff4d4f",
      fontSize: 10,
      fontWeight: "700",
    },

    badLineTooltip: {
      backgroundColor: "#1a1a1a",
      borderColor: "#ff4d4f",
      borderWidth: 1,
      borderRadius: 12,
      padding: 14,
      marginTop: 8,
      marginBottom: 8,
    },

    tooltipTitle: {
      color: "#ff4d4f",
      fontWeight: "700",
      fontSize: 14,
      marginBottom: 6,
    },

    tooltipText: {
      color: "#ddd",
      fontSize: 13,
      lineHeight: 18,
      marginBottom: 8,
    },

    tooltipMeta: {
      color: "#888",
      fontSize: 12,
      marginBottom: 8,
    },

    tooltipClose: {
      alignSelf: "flex-end",
      paddingVertical: 4,
      paddingHorizontal: 8,
    },

    tooltipCloseText: {
      color: "#888",
      fontSize: 12,
      fontWeight: "600",
    },
  });
}
