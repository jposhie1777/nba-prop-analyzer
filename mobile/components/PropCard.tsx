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
import { useMemo, useState } from "react";

import { useTheme } from "@/store/useTheme";
import { BOOKMAKER_LOGOS } from "@/utils/bookmakerLogos";
import { MiniBarSparkline } from "@/components/sparkline/MiniBarSparkline";
import { formatMarketLabel } from "@/utils/formatMarket";

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

  market: string;
  side?: "over" | "under" | "yes";
  line: number;
  odds: number;

  bookmaker?: string;
  books?: BookOdds[];

  confidence?: number;

  avg_l5?: number;
  avg_l10?: number;
  avg_l20?: number;

  bad_miss_pct_l5?: number;
  bad_miss_pct_l10?: number;
  bad_miss_pct_l20?: number;

  pace_l5?: number;
  pace_l10?: number;
  pace_l20?: number;

  sparkline_l5?: number[];
  sparkline_l10?: number[];
  sparkline_l20?: number[];

  last5_dates?: string[];
  last10_dates?: string[];
  last20_dates?: string[];

  hitRate: number;
  hitRatePct: number;
  window?: "L5" | "L10" | "L20";

  saved: boolean;
  onToggleSave: () => void;

  // ✅ swipe-right save
  onSwipeSave?: () => void;

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

  /* =========================
     BOOK
  ========================= */
  const resolvedBook = useMemo(() => {
    if (books?.length) return books[0];
    if (bookmaker) return { bookmaker, odds };
    return null;
  }, [books, bookmaker, odds]);

  const bookLogo =
    resolvedBook &&
    BOOKMAKER_LOGOS[normalizeBookKey(resolvedBook.bookmaker)];

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
      overshootRight={false}
      simultaneousHandlers={scrollRef}
      renderLeftActions={renderSwipeSave}
      onSwipeableLeftOpen={() => {
        if (!saved) {
          onSwipeSave?.();
        }
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

          {/* HEADER */}
          <Pressable onPress={onToggleExpand} style={styles.headerRow}>
            {imageUrl && (
              <Image
                source={{ uri: imageUrl }}
                style={styles.headshot}
                resizeMode="cover"
              />
            )}

            <View style={styles.headerText}>
              <Text style={styles.player}>{player}</Text>

              <Text style={styles.marketLine}>
                {formatMarketLabel(market)} • {formatSideLabel(side)} {line}
                {odds != null && (
                  <Text style={styles.oddsText}>  {formatOdds(odds)}</Text>
                )}
              </Text>

              {bookLogo && (
                <Image
                  source={{ uri: bookLogo }}
                  style={styles.bookLogo}
                  resizeMode="contain"
                />
              )}

              <Text style={styles.hitText}>{hitRatePct}% HIT</Text>
              <Text style={styles.metricSub}>Last {displayWindow}</Text>
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

    saveButton: {
      position: "absolute",
      top: 8,
      right: 8,
      zIndex: 2,
    },

    saveStar: { fontSize: 18, color: colors.text.muted },
    saveStarOn: { color: colors.accent.primary },

    headerRow: {
      flexDirection: "row",
      alignItems: "center",
      gap: 12,
    },

    headshot: {
      width: 44,
      height: 44,
      borderRadius: 22,
      backgroundColor: colors.surface.cardSoft,
    },

    headerText: { flex: 1 },

    player: {
      fontWeight: "800",
      fontSize: 15,
      color: colors.text.primary,
    },

    marketLine: {
      marginTop: 2,
      color: colors.text.secondary,
      fontSize: 13,
    },

    oddsText: {
      fontWeight: "700",
      color: colors.text.primary,
    },

    bookLogo: {
      width: 42,
      height: 16,
      marginTop: 4,
      opacity: 0.9,
    },

    hitText: {
      marginTop: 6,
      fontWeight: "900",
      color: colors.text.primary,
    },

    metricSub: {
      color: colors.text.muted,
      fontSize: 12,
    },

    expandWrap: { marginTop: 12 },

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

    /* SWIPE */
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
    },
  });
}