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
import { useMemo, useState, useRef, useEffect } from "react";
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
  playerPosition?: string;
  opponentTeamAbbr?: string;
  opponentPositionRank?: number;
  teamPace?: number;
  teamPaceRank?: number;
  opponentPace?: number;
  opponentStatAllowed?: number;
  opponentStatAllowedIsRate?: boolean;
  opponentPaceRank?: number;
  impliedTeamTotal?: number;
  spread?: number;
  minutesAvg?: number;
  usagePct?: number;
  lineMovement?: string;
  oddsMovement?: string;
  wowyStatLabel?: string;
  wowyImpacts?: WowyImpactDisplay[];

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

type WowyImpactDisplay = {
  injuredPlayerId: number;
  injuredPlayerName: string;
  injuredStatus?: string;
  diff: number | null;
  statWith: number | null;
  statWithout: number | null;
  gamesWith: number | null;
  gamesWithout: number | null;
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

function normalizeTeamKey(team?: string) {
  if (!team) return undefined;
  return team.trim().toUpperCase();
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

function formatDiff(value: number | null): string {
  if (value === null || value === undefined) return "—";
  const sign = value > 0 ? "+" : "";
  return `${sign}${value.toFixed(1)}`;
}

function formatAllowedStat(value?: number, isRate?: boolean): string {
  if (value === null || value === undefined) return "—";
  if (isRate) return `${(value * 100).toFixed(0)}%`;
  return value.toFixed(1);
}

function getDiffColor(value: number | null, colors: any): string {
  if (value === null || value === undefined) return colors.text.muted;
  if (value > 2) return "#22c55e";
  if (value > 0) return "#4ade80";
  if (value < -2) return "#ef4444";
  if (value < 0) return "#f87171";
  return colors.text.muted;
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
    homeTeam,
    awayTeam,
    playerPosition,
    opponentTeamAbbr,
    opponentPositionRank,
    teamPace,
    teamPaceRank,
    opponentPace,
    opponentStatAllowed,
    opponentStatAllowedIsRate,
    opponentPaceRank,
    impliedTeamTotal,
    spread,
    minutesAvg,
    usagePct,
    lineMovement,
    oddsMovement,
    wowyStatLabel,
    wowyImpacts,
  } = props;

  /* =========================
     DISPLAY WINDOW
  ========================= */
  const [displayWindow, setDisplayWindow] = useState<"L5" | "L10" | "L20">(
    props.window ?? "L10"
  );

  useEffect(() => {
    if (props.window) {
      setDisplayWindow(props.window);
    }
  }, [props.window]);

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

  // 🔧 normalize BEFORE logo lookup
  function normalizeTeamKey(team?: string) {
    if (!team) return undefined;
    const key = team.trim().toUpperCase();
    if (key === "NO") return "NOP";
    if (key === "PHO") return "PHX";
    return key;
  }

  const homeKey = normalizeTeamKey(homeTeam);
  const awayKey = normalizeTeamKey(awayTeam);

  const homeLogo = homeKey ? TEAM_LOGOS[homeKey] : null;
  const awayLogo = awayKey ? TEAM_LOGOS[awayKey] : null;

  if (__DEV__ && (homeKey && !homeLogo || awayKey && !awayLogo)) {
    console.warn("🚨 TEAM LOGO MISS", {
      homeTeam,
      awayTeam,
      homeKey,
      awayKey,
    });
  }

    
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
          {/* LEFT ACCENT BAR */}
          <View style={[styles.accentBar, { backgroundColor: hitRateColor }]} />

          {/* CARD BODY */}
          <View style={styles.cardBody}>
            {/* MAIN CONTENT */}
            <Pressable onPress={onToggleExpand} style={styles.contentWrap}>
            {/* TOP ROW: MATCHUP + ODDS */}
            <View style={styles.topRow}>
              {/* MATCHUP LOGOS */}
              <View style={styles.matchupRow}>
                {awayLogo ? (
                  <Image source={{ uri: awayLogo }} style={styles.teamLogo} />
                ) : awayTeam ? (
                  <Text style={styles.teamAbbr}>{awayTeam}</Text>
                ) : null}
                <Text style={styles.atSymbol}>@</Text>
                {homeLogo ? (
                  <Image source={{ uri: homeLogo }} style={styles.teamLogo} />
                ) : homeTeam ? (
                  <Text style={styles.teamAbbr}>{homeTeam}</Text>
                ) : null}
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

            {/* PLAYER ROW */}
            <View style={styles.playerRow}>
              {/* HEADSHOT */}
              <View style={styles.headshotWrap}>
                {imageUrl ? (
                  <Image source={{ uri: imageUrl }} style={styles.headshot} />
                ) : (
                  <View style={styles.headshotPlaceholder} />
                )}
              </View>

              {/* PLAYER INFO */}
              <View style={styles.playerInfo}>
                <Text style={styles.player} numberOfLines={1}>{player}</Text>
                <Text style={styles.marketLine}>
                  {formatMarketLabel(market)} · {formatSideLabel(side)} {line}
                </Text>
              </View>
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
              <View style={styles.statBlock}>
                <Text style={styles.statLabel}>{displayWindow} HIT</Text>
                <Text style={styles.statValue}>
                  {hitRate != null ? `${hitRate}%` : "—"}
                </Text>
              </View>

              <View style={styles.statBlockRight}>
                <Text style={styles.statLabel}>{displayWindow} AVG</Text>
                <Text style={styles.avgValue}>
                  {windowAvg != null ? windowAvg.toFixed(1) : "—"}
                </Text>
              </View>
            </View>

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
          </Pressable>

            {/* EXPANDED */}
            {expanded && (
              <View style={styles.expandWrap}>
                {(playerPosition || opponentTeamAbbr || opponentPositionRank != null) && (
                  <View style={styles.opponentWrap}>
                    <View>
                      <Text style={styles.opponentTitle}>
                        Opponent Position Rank
                      </Text>
                      {opponentTeamAbbr && playerPosition && (
                        <Text style={styles.opponentSubtitle}>
                          {opponentTeamAbbr} vs {playerPosition}
                        </Text>
                      )}
                      {opponentStatAllowed != null && (
                        <Text style={styles.opponentAllowed}>
                          Allowed {formatMarketLabel(market)}:{" "}
                          {formatAllowedStat(
                            opponentStatAllowed,
                            opponentStatAllowedIsRate,
                          )}
                        </Text>
                      )}
                    </View>
                    <View style={styles.opponentRankChip}>
                      <Text style={styles.opponentRankLabel}>
                        {formatMarketLabel(market)} Rk
                      </Text>
                      <Text style={styles.opponentRankValue}>
                        {opponentPositionRank != null
                          ? `#${opponentPositionRank}`
                          : "—"}
                      </Text>
                    </View>
                  </View>
                )}
                {(teamPaceRank != null ||
                  teamPace != null ||
                  opponentPaceRank != null ||
                  opponentPace != null ||
                  impliedTeamTotal != null ||
                  spread != null) && (
                  <View style={styles.chipRow}>
                    {(teamPaceRank != null || teamPace != null) && (
                      <View style={styles.metricChip}>
                        <Text style={styles.metricChipLabel}>Pace</Text>
                        <Text style={styles.metricChipValue}>
                          {teamPaceRank != null
                            ? `#${teamPaceRank}`
                            : teamPace?.toFixed(1)}
                        </Text>
                      </View>
                    )}
                    {(opponentPaceRank != null || opponentPace != null) && (
                      <View style={styles.metricChip}>
                        <Text style={styles.metricChipLabel}>Opp Pace</Text>
                        <Text style={styles.metricChipValue}>
                          {opponentPaceRank != null
                            ? `#${opponentPaceRank}`
                            : opponentPace?.toFixed(1)}
                  opponentPaceRank != null ||
                  impliedTeamTotal != null ||
                  spread != null) && (
                  <View style={styles.chipRow}>
                    {teamPaceRank != null && (
                      <View style={styles.metricChip}>
                        <Text style={styles.metricChipLabel}>Pace</Text>
                        <Text style={styles.metricChipValue}>#{teamPaceRank}</Text>
                      </View>
                    )}
                    {opponentPaceRank != null && (
                      <View style={styles.metricChip}>
                        <Text style={styles.metricChipLabel}>Opp Pace</Text>
                        <Text style={styles.metricChipValue}>
                          #{opponentPaceRank}
                        </Text>
                      </View>
                    )}
                    {impliedTeamTotal != null && (
                      <View style={styles.metricChip}>
                        <Text style={styles.metricChipLabel}>Team TT</Text>
                        <Text style={styles.metricChipValue}>
                          {impliedTeamTotal.toFixed(1)}
                        </Text>
                      </View>
                    )}
                    {spread != null && (
                      <View style={styles.metricChip}>
                        <Text style={styles.metricChipLabel}>Spread</Text>
                        <Text style={styles.metricChipValue}>
                          {spread > 0 ? `+${spread}` : spread}
                        </Text>
                      </View>
                    )}
                  </View>
                )}

                {(minutesAvg != null || usagePct != null) && (
                  <View style={styles.chipRow}>
                    {minutesAvg != null && (
                      <View style={styles.metricChip}>
                        <Text style={styles.metricChipLabel}>MPG</Text>
                        <Text style={styles.metricChipValue}>
                          {minutesAvg.toFixed(1)}
                        </Text>
                      </View>
                    )}
                    {usagePct != null && (
                      <View style={styles.metricChip}>
                        <Text style={styles.metricChipLabel}>USG</Text>
                        <Text style={styles.metricChipValue}>
                          {usagePct.toFixed(0)}%
                        </Text>
                      </View>
                    )}
                  </View>
                )}

                {(lineMovement || oddsMovement) && (
                  <View style={styles.marketRow}>
                    {lineMovement && (
                      <View style={styles.marketItem}>
                        <Text style={styles.marketLabel}>Line</Text>
                        <Text style={styles.marketValue}>{lineMovement}</Text>
                      </View>
                    )}
                    {oddsMovement && (
                      <View style={styles.marketItem}>
                        <Text style={styles.marketLabel}>Odds</Text>
                        <Text style={styles.marketValue}>{oddsMovement}</Text>
                      </View>
                    )}
                  </View>
                )}
                <MiniBarSparkline data={sparkline} dates={dates} />
                {wowyImpacts && (
                  <View style={styles.wowySection}>
                    <Text style={styles.wowyTitle}>
                      WOWY Impact{wowyStatLabel ? ` · ${wowyStatLabel}` : ""}
                    </Text>
                    {wowyImpacts.length > 0 ? (
                      wowyImpacts.map((impact) => {
                        const diffColor = getDiffColor(impact.diff, colors);
                        const statusParts = [impact.injuredStatus].filter(Boolean);
                        return (
                          <View
                            key={`${impact.injuredPlayerId}-${impact.injuredPlayerName}`}
                            style={styles.wowyRow}
                          >
                            <View style={styles.wowyLeft}>
                              <Text style={styles.wowyName}>
                                {impact.injuredPlayerName}
                              </Text>
                              {statusParts.length > 0 && (
                                <Text style={styles.wowyMeta}>
                                  {statusParts.join(" • ")}
                                </Text>
                              )}
                            </View>
                            <View style={styles.wowyRight}>
                              <Text style={[styles.wowyDiff, { color: diffColor }]}>
                                {formatDiff(impact.diff)}
                              </Text>
                              <Text style={styles.wowyValue}>
                                {impact.statWithout != null
                                  ? impact.statWithout.toFixed(1)
                                  : "—"}
                                {" vs "}
                                {impact.statWith != null
                                  ? impact.statWith.toFixed(1)
                                  : "—"}
                              </Text>
                              {impact.gamesWith != null &&
                                impact.gamesWithout != null && (
                                  <Text style={styles.wowyGames}>
                                    {impact.gamesWithout} w/o • {impact.gamesWith} w/
                                  </Text>
                                )}
                            </View>
                          </View>
                        );
                      })
                    ) : (
                      <Text style={styles.wowyEmpty}>
                        No WOWY impact data for this market.
                      </Text>
                    )}
                  </View>
                )}

              </View>
            )}
          </View>
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
      flexDirection: "row",
      backgroundColor: colors.surface.card,
      borderRadius: 16,
      borderWidth: 1.5,
      borderColor: colors.border.subtle,
      shadowColor: "#000",
      shadowOffset: { width: 0, height: 2 },
      shadowOpacity: 0.08,
      shadowRadius: 8,
      elevation: 3,
      overflow: "hidden",
    },

    accentBar: {
      width: 4,
    },

    cardBody: {
      flex: 1,
    },

    contentWrap: {
      padding: 14,
    },

    /* =========================
       TOP ROW (MATCHUP + ODDS)
    ========================= */
    topRow: {
      flexDirection: "row",
      justifyContent: "space-between",
      alignItems: "center",
      marginBottom: 12,
    },

    matchupRow: {
      flexDirection: "row",
      alignItems: "center",
      gap: 6,
    },

    teamLogo: {
      width: 24,
      height: 24,
      resizeMode: "contain",
    },

    teamAbbr: {
      fontSize: 12,
      fontWeight: "700",
      color: colors.text.muted,
    },

    atSymbol: {
      fontSize: 11,
      fontWeight: "700",
      color: colors.text.muted,
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
       PLAYER ROW
    ========================= */
    playerRow: {
      flexDirection: "row",
      alignItems: "center",
      marginBottom: 12,
    },

    headshotWrap: {
      width: 44,
      height: 44,
      marginRight: 12,
    },

    headshot: {
      width: 44,
      height: 44,
      borderRadius: 22,
      backgroundColor: colors.surface.cardSoft,
    },

    headshotPlaceholder: {
      width: 44,
      height: 44,
      borderRadius: 22,
      backgroundColor: colors.surface.cardSoft,
    },

    playerInfo: {
      flex: 1,
    },

    player: {
      fontSize: 16,
      fontWeight: "700",
      color: colors.text.primary,
      marginBottom: 2,
    },

    marketLine: {
      fontSize: 13,
      color: colors.text.muted,
    },

    /* =========================
       STATS ROW
    ========================= */
    statsRow: {
      flexDirection: "row",
      justifyContent: "space-between",
      marginTop: 4,
    },

    statBlock: {
      alignItems: "flex-start",
    },

    statBlockRight: {
      alignItems: "flex-end",
    },

    statLabel: {
      fontSize: 11,
      fontWeight: "700",
      color: colors.text.muted,
      marginBottom: 2,
    },

    statValue: {
      fontSize: 16,
      fontWeight: "900",
      color: colors.accent.primary,
    },

    avgValue: {
      fontSize: 16,
      fontWeight: "900",
      color: colors.accent.primary,
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

    chipRow: {
      flexDirection: "row",
      flexWrap: "wrap",
      gap: 8,
      marginBottom: 10,
    },

    metricChip: {
      backgroundColor: colors.surface.cardSoft,
      paddingHorizontal: 10,
      paddingVertical: 6,
      borderRadius: 10,
    },

    metricChipLabel: {
      fontSize: 10,
      color: colors.text.muted,
      fontWeight: "700",
    },

    metricChipValue: {
      fontSize: 13,
      fontWeight: "800",
      color: colors.text.primary,
      marginTop: 2,
    },

    marketRow: {
      flexDirection: "row",
      flexWrap: "wrap",
      gap: 12,
      marginBottom: 12,
    },

    marketItem: {
      flexDirection: "row",
      alignItems: "center",
      gap: 6,
    },

    marketLabel: {
      fontSize: 11,
      fontWeight: "700",
      color: colors.text.muted,
    },

    marketValue: {
      fontSize: 12,
      fontWeight: "700",
      color: colors.text.primary,
    },

    opponentWrap: {
      flexDirection: "row",
      justifyContent: "space-between",
      alignItems: "center",
      marginBottom: 12,
      gap: 12,
    },

    opponentTitle: {
      fontSize: 12,
      fontWeight: "700",
      color: colors.text.primary,
    },

    opponentSubtitle: {
      fontSize: 11,
      color: colors.text.muted,
      marginTop: 2,
    },

    opponentAllowed: {
      fontSize: 11,
      color: colors.text.muted,
      marginTop: 4,
    },

    opponentRankChip: {
      alignItems: "flex-end",
      backgroundColor: colors.surface.cardSoft,
      paddingHorizontal: 10,
      paddingVertical: 6,
      borderRadius: 10,
    },

    opponentRankLabel: {
      fontSize: 10,
      color: colors.text.muted,
      fontWeight: "700",
    },

    opponentRankValue: {
      fontSize: 14,
      fontWeight: "800",
      color: colors.accent.primary,
    },

    wowySection: {
      marginTop: 14,
      paddingTop: 12,
      borderTopWidth: 1,
      borderTopColor: colors.border.subtle,
      gap: 10,
    },

    wowyTitle: {
      fontSize: 12,
      fontWeight: "700",
      color: colors.text.primary,
      textTransform: "uppercase",
      letterSpacing: 0.6,
    },

    wowyRow: {
      flexDirection: "row",
      justifyContent: "space-between",
      gap: 12,
    },

    wowyLeft: {
      flex: 1,
    },

    wowyName: {
      fontSize: 13,
      fontWeight: "700",
      color: colors.text.primary,
    },

    wowyMeta: {
      marginTop: 2,
      fontSize: 11,
      color: colors.text.muted,
    },

    wowyRight: {
      alignItems: "flex-end",
    },

    wowyDiff: {
      fontSize: 14,
      fontWeight: "800",
    },

    wowyValue: {
      marginTop: 2,
      fontSize: 11,
      color: colors.text.muted,
    },

    wowyGames: {
      marginTop: 2,
      fontSize: 10,
      color: colors.text.muted,
    },

    wowyEmpty: {
      fontSize: 12,
      color: colors.text.muted,
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
      borderRadius: 16,
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
      borderRadius: 6,
      alignSelf: "flex-start",
      marginBottom: 8,
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
