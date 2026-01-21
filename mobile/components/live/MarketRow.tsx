// components/live/MarketRow.tsx
import React, { useRef, useEffect, useState } from "react";
import { View, Text, ScrollView, StyleSheet } from "react-native";
import { LineButton } from "./LineButton";
import { OverUnderButton } from "./OverUnderButton";
import { useSavedBets } from "@/store/useSavedBets";
import { fetchLivePropAnalytics } from "@/services/liveAnalytics";
import { openTrendChart } from "@/navigation/trendLinking";
import { Pressable } from "react-native";

/* ======================================================
   MARKET LABELS
====================================================== */
const MARKET_LABELS: Record<string, string> = {
  pts: "POINTS",
  reb: "REBOUNDS",
  ast: "ASSISTS",
  "3pm": "THREES",
};

const TREND_MARKET_MAP: Record<string, string> = {
  pts: "pts",
  reb: "reb",
  ast: "ast",
  "3pm": "fg3m",
};

export function MarketRow({
  market,
  lines,
  current,
  playerName,
  playerId,
}: any) {
  const toggleSave = useSavedBets((s) => s.toggleSave);
  const savedIds = useSavedBets((s) => s.savedIds);

  const scrollRef = useRef<ScrollView>(null);
  const buttonWidthRef = useRef<number>(0);
  const didAutoScroll = useRef(false);

  const marketKey =
    TREND_MARKET_MAP[market] ?? market;
  const marketLabel = MARKET_LABELS[market] ?? market.toUpperCase();

  /* ======================
     BOOK ODDS NORMALIZATION
  ====================== */
  const getBookOdds = (
    line: any,
    book: "draftkings" | "fanduel" = "draftkings"
  ) => {
    const b = line.books?.[book];
    return {
      over:
        b?.over ??
        line.over_odds ??
        (line.side === "over" ? line.price : null),
      under:
        b?.under ??
        line.under_odds ??
        (line.side === "under" ? line.price : null),
      milestone: b?.milestone ?? null,
    };
  };

  /* ======================
     MAIN O/U LINE
  ====================== */
  const overUnderByLine = new Map<number, any>();
  for (const l of lines) {
    if (l.line_type === "over_under") {
      overUnderByLine.set(l.line, l);
    }
  }

  const mainLine = Array.from(overUnderByLine.values()).find((l) => {
    const { over, under } = getBookOdds(l);
    return over != null || under != null;
  });

  /* ======================
     MILESTONES
  ====================== */
  const MAX_STEPS_AHEAD = 7;

  const milestones = lines
    .filter(
      (l: any) =>
        l.line_type === "milestone" &&
        l.line > current &&
        getBookOdds(l).milestone != null
    )
    .sort((a: any, b: any) => a.line - b.line)
    .slice(0, MAX_STEPS_AHEAD);

  const getState = (lineValue: number) => {
    const remaining = lineValue - current;
    if (remaining <= 0) return "hit";
    if (remaining === 1) return "close";
    return "pending";
  };

  /* ======================
     AUTO SCROLL TO CLOSE
  ====================== */
  const closeIndex = milestones.findIndex(
    (m: any) => getState(m.line) === "close"
  );

  useEffect(() => {
    if (
      didAutoScroll.current ||
      closeIndex < 0 ||
      !scrollRef.current ||
      buttonWidthRef.current === 0
    ) {
      return;
    }

    scrollRef.current.scrollTo({
      x: closeIndex * (buttonWidthRef.current + 8),
      animated: true,
    });

    didAutoScroll.current = true;
  }, [closeIndex]);

  /* ======================
     ANALYTICS EXPAND
  ====================== */
  const [expandedLine, setExpandedLine] = useState<{ line: number } | null>(null);
  const [analytics, setAnalytics] = useState<any | null>(null);
  const [loadingAnalytics, setLoadingAnalytics] = useState(false);

  const toggleExpand = async (line: number) => {
    if (expandedLine?.line === line) {
      setExpandedLine(null);
      setAnalytics(null);
      return;
    }

    setExpandedLine({ line });
    setAnalytics(null);
    setLoadingAnalytics(true);

    try {
      const result = await fetchLivePropAnalytics({
        gameId: mainLine?.game_id ?? line.game_id,
        playerId,
        market: marketKey,
        line,
        side: mainLine?.line === line ? "over" : "milestone",
      });
      setAnalytics(result);
    } catch {
      setAnalytics(null);
    } finally {
      setLoadingAnalytics(false);
    }
  };

  if (!mainLine && milestones.length === 0) {
    return null;
  }

  return (
    <View style={styles.wrap}>
      {/* MARKET HEADER */}
      <View style={styles.marketHeader}>
        <Text style={styles.marketLabel}>{marketLabel}</Text>
      
        <Pressable
          onPress={() => openTrendChart(playerName, marketKey)}
          hitSlop={8}
        >
          <Text style={styles.trendLink}>TREND</Text>
        </Pressable>
      </View>

      {/* MAIN O/U */}
      {mainLine && (
        <>
          <Text style={styles.sectionLabel}>MAIN LINE</Text>
          <ScrollView
            horizontal
            showsHorizontalScrollIndicator={false}
            contentContainerStyle={styles.ouRow}
          >
            {(() => {
              const { over, under } = getBookOdds(mainLine);

              return (
                <>
                  <OverUnderButton
                    side="over"
                    line={mainLine.line}
                    odds={over}
                    disabled={over == null}
                    isSelected={savedIds.has(
                      `ou:${mainLine.game_id}:${playerId}:${marketKey}:over:${mainLine.line}`
                    )}
                    onPress={() =>
                      over != null &&
                      toggleSave({
                        id: `ou:${mainLine.game_id}:${playerId}:${marketKey}:over:${mainLine.line}`,
                        player: playerName,
                        playerId,
                        gameId: mainLine.game_id,
                        market: marketKey,
                        line: mainLine.line,
                        side: "over",
                        odds: over,
                      })
                    }
                  />

                  <OverUnderButton
                    side="under"
                    line={mainLine.line}
                    odds={under}
                    disabled={under == null}
                    isSelected={savedIds.has(
                      `ou:${mainLine.game_id}:${playerId}:${marketKey}:under:${mainLine.line}`
                    )}
                    onPress={() =>
                      under != null &&
                      toggleSave({
                        id: `ou:${mainLine.game_id}:${playerId}:${marketKey}:under:${mainLine.line}`,
                        player: playerName,
                        playerId,
                        gameId: mainLine.game_id,
                        market: marketKey,
                        line: mainLine.line,
                        side: "under",
                        odds: under,
                      })
                    }
                  />
                </>
              );
            })()}
          </ScrollView>
        </>
      )}

      {/* MILESTONES */}
      {milestones.length > 0 && (
        <>
          <Text style={styles.sectionLabel}>MILESTONES</Text>

          <ScrollView
            ref={scrollRef}
            horizontal
            showsHorizontalScrollIndicator={false}
            contentContainerStyle={{ paddingRight: 12 }}
            onScrollBeginDrag={() => setExpandedLine(null)}
          >
            <View style={styles.milestoneRow}>
              {milestones.map((m: any, idx: number) => (
                <View
                  key={`ms-${playerId}-${marketKey}-${m.line}`}
                  onLayout={
                    idx === 0
                      ? (e) =>
                          (buttonWidthRef.current =
                            e.nativeEvent.layout.width)
                      : undefined
                  }
                >
                  <LineButton
                    line={m}
                    market={market}
                    playerId={playerId}
                    state={getState(m.line)}
                    isSelected={savedIds.has(
                      `ms:${m.game_id}:${playerId}:${marketKey}:${m.line}`
                    )}
                    isExpanded={expandedLine?.line === m.line}
                    onSave={() =>
                      toggleSave({
                        id: `ms:${m.game_id}:${playerId}:${marketKey}:${m.line}`,
                        player: playerName,
                        playerId,
                        gameId: m.game_id,
                        market: marketKey,
                        line: m.line,
                        side: "milestone",
                        odds: getBookOdds(m).milestone ?? undefined,
                      })
                    }
                    onInspect={() => toggleExpand(m.line)}
                  />
                </View>
              ))}
            </View>
          </ScrollView>
        </>
      )}

      {/* ANALYTICS STRIP (UNCHANGED) */}
      {expandedLine?.line != null && (
        <View style={styles.analytics}>
          {loadingAnalytics && (
            <Text style={styles.analyticsText}>Loading analytics…</Text>
          )}

          {!loadingAnalytics && analytics && (
            <>
              <Text style={styles.analyticsText}>
                Fair odds: {analytics.fair_odds ?? "—"}
              </Text>
              <Text style={styles.analyticsText}>
                On pace for{" "}
                {analytics.on_pace_value?.toFixed(1) ?? "—"} {market}
              </Text>
              <Text style={styles.analyticsText}>
                Δ vs pace: {analytics.delta_vs_pace?.toFixed(1) ?? "—"}
              </Text>
            </>
          )}
        </View>
      )}
    </View>
  );
}

/* ======================================================
   STYLES
====================================================== */
const styles = StyleSheet.create({
  wrap: {
    marginTop: 8,
  },

  marketLabel: {
    fontSize: 12,
    fontWeight: "900",
    letterSpacing: 1,
    textAlign: "center",
    marginBottom: 6,
  },

  sectionLabel: {
    fontSize: 10,
    fontWeight: "800",
    letterSpacing: 0.8,
    opacity: 0.6,
    marginBottom: 4,
    marginLeft: 2,
  },

  ouRow: {
    gap: 8,
    paddingVertical: 4,
  },

  milestoneRow: {
    flexDirection: "row",
    gap: 8,
    flexWrap: "nowrap",
  },

  analytics: {
    marginTop: 8,
    padding: 8,
    borderRadius: 8,
    backgroundColor: "#00000011",
  },

  analyticsText: {
    fontSize: 11,
    opacity: 0.85,
  },
  marketHeader: {
    flexDirection: "row",
    alignItems: "center",
    justifyContent: "space-between",
    marginBottom: 6,
  },
  
  trendLink: {
    fontSize: 10,
    fontWeight: "900",
    letterSpacing: 0.6,
    opacity: 0.7,
  },
});