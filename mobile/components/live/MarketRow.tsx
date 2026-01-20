// components/live/marketrow
import React, { useRef, useEffect, useState } from "react";
import { View, Text, ScrollView, StyleSheet } from "react-native";
import { LineButton } from "./LineButton";
import { OverUnderButton } from "./OverUnderButton";
import { useSavedBets } from "@/store/useSavedBets";
import { fetchLivePropAnalytics } from "@/services/liveAnalytics";


export function MarketRow({
  market,
  lines,
  current,
  playerName,
  playerId, // 👈 ADD
}: any) {
  const toggleSave = useSavedBets((s) => s.toggleSave);
  const savedIds = useSavedBets((s) => s.savedIds);
  const scrollRef = useRef<ScrollView>(null);
  const buttonWidthRef = useRef<number>(0);
  const overUnderByLine = new Map<number, any>();
  const marketKey = market; // canonical
  const marketLabel = market.toUpperCase();

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

  const didAutoScroll = useRef(false);

  for (const l of lines) {
    if (l.line_type !== "over_under") continue;
    overUnderByLine.set(l.line, l);
  }
  
  const mainLine = Array.from(overUnderByLine.values()).find((l) => {
    const { over, under } = getBookOdds(l);
    return over != null || under != null;
  });
  
  /* ======================
     DEBUG — mainLine miss
  ====================== */
  if (__DEV__ && !mainLine) {
    console.warn("[MarketRow] No main OU line resolved", {
      market,
      playerId,
      lineCount: lines?.length,
      sampleLine: lines?.[0],
      normalizedOdds: lines?.map((l: any) => ({
        line: l.line,
        line_type: l.line_type,
        over: getBookOdds(l).over,
        under: getBookOdds(l).under,
        raw: {
          price: l.price,
          over_odds: l.over_odds,
          under_odds: l.under_odds,
          books: l.books,
        },
      })),
    });
  }

  const getState = (lineValue: number) => {
    const remaining = lineValue - current;
    if (remaining <= 0) return "hit";
    if (remaining === 1) return "close";
    return "pending";
  };

  const getMilestoneOdds = (line: any): number | null => {
    const { milestone } = getBookOdds(line);
    return milestone;
  };

  const MAX_STEPS_AHEAD = 7;

  const milestones = lines
    .filter(
      (l: any) =>
        l.line_type === "milestone" &&
        l.line > current &&
        getMilestoneOdds(l) !== null
    )
    .sort((a: any, b: any) => a.line - b.line)
    .slice(0, MAX_STEPS_AHEAD);
  
  const getOverUnderBetId = (
    side: "over" | "under",
    line: number
  ) => {
    if (!mainLine) return null;
    return `ou:${mainLine.game_id}:${playerId}:${marketKey}:${side}:${line}`;
  };
  
  const getMilestoneBetId = (m: any) => {
    return `ms:${m.game_id}:${playerId}:${marketKey}:${m.line}`;
  };

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
  
  const [expandedLine, setExpandedLine] = useState<{ line: number } | null>(null);
  const [analytics, setAnalytics] = useState<any | null>(null);
  const [loadingAnalytics, setLoadingAnalytics] = useState(false);

  const toggleExpand = async (line: number) => {
    // collapse
    if (expandedLine?.line === line) {
      setExpandedLine(null);
      setAnalytics(null);
      return;
    }

    setExpandedLine({ line });
    setAnalytics(null);
    setLoadingAnalytics(true);

    // determine side (safe defaults)
    const side =
      mainLine?.line === line
        ? "over"        // main OU line → over analytics
        : "milestone";  // milestone buttons

    try {
      const result = await fetchLivePropAnalytics({
        gameId: mainLine?.game_id ?? line.game_id,
        playerId,
        market: marketKey,
        line,
        side,
      });

      setAnalytics(result);
    } catch {
      setAnalytics(null);
    } finally {
      setLoadingAnalytics(false);
    }
  };

  
  // 🔒 collapse inspection when market changes
  useEffect(() => {
    setExpandedLine(null);
    setAnalytics(null);
  }, [market]);

  const hasOU = !!mainLine;
  const hasMilestones = milestones.length > 0;
  
  /* ======================
     DEV GUARD — market hidden
  ====================== */
  if (__DEV__ && !hasOU && !hasMilestones) {
    console.warn("[MarketRow] Market hidden (no render)", {
      market,
      playerId,
      lines,
    });
  }
  
  if (!hasOU && !hasMilestones) {
    return null;
  }

  return (
    <View>
      <Text style={styles.label}>{marketLabel}</Text>

      {/* MAIN OVER / UNDER */}
      {mainLine && (
        <View style={{ marginBottom: 6 }}>
          <ScrollView
            horizontal
            showsHorizontalScrollIndicator={false}
            contentContainerStyle={{
              gap: 8,
              paddingVertical: 4,
            }}
          >
            {(() => {
              const { over, under } = getBookOdds(mainLine);
          
              const overBetId = getOverUnderBetId("over", mainLine.line);
              const underBetId = getOverUnderBetId("under", mainLine.line);
          
              return (
                <>
                  <OverUnderButton
                    side="over"
                    line={mainLine.line}
                    odds={over}
                    disabled={over == null}
                    isSelected={savedIds.has(overBetId)}
                    onPress={() => {
                      if (over == null) return;
                      toggleSave({
                        id: overBetId,
                        player: playerName,
                        playerId: mainLine.player_id,
                        gameId: mainLine.game_id,
                        market: marketKey,
                        line: mainLine.line,
                        side: "over",
                        odds: over,
                      });
                    }}
                  />
          
                  <OverUnderButton
                    side="under"
                    line={mainLine.line}
                    odds={under}
                    disabled={under == null}
                    isSelected={savedIds.has(underBetId)}
                    onPress={() => {
                      if (under == null) return;
                      toggleSave({
                        id: underBetId,
                        player: playerName,
                        playerId: mainLine.player_id,
                        gameId: mainLine.game_id,
                        market: marketKey,
                        line: mainLine.line,
                        side: "under",
                        odds: under,
                      });
                    }}
                  />
                </>
              );
            })()}
          </ScrollView>
        </View>
      )}

      {/* MILESTONES */}
      {milestones.length > 0 && (
        <View style={{ marginTop: 6 }}>
          <ScrollView
            ref={scrollRef}
            horizontal
            showsHorizontalScrollIndicator={false}
            contentContainerStyle={{ paddingRight: 12 }}
            onScrollBeginDrag={() => setExpandedLine(null)} // 👈 ADD HERE
          >
            <View style={styles.row}>
              {milestones.map((m: any, idx: number) => {
                const betId = getMilestoneBetId(m);
                const isSelected = savedIds.has(betId);
                const isExpanded = expandedLine?.line === m.line;
      
                return (
                  <View
                    key={`ms-${playerId}-${marketKey}-${m.line}`}
                    onLayout={
                      idx === 0
                        ? (e) => {
                            buttonWidthRef.current =
                              e.nativeEvent.layout.width;
                          }
                        : undefined
                    }
                  >
                    <LineButton
                      line={m}
                      market={market}
                      playerId={playerId}
                      state={getState(m.line)}
                      isSelected={isSelected}
                      isExpanded={isExpanded}
                      onSave={() => {
                        toggleSave({
                          id: betId,
                          player: playerName,
                          playerId: playerId,
                          gameId: m.game_id,
                          market: marketKey,
                          line: m.line,
                          side: "milestone",
                          odds: getMilestoneOdds(m) ?? undefined,
                        });
                      }}
                      onInspect={() => toggleExpand(m.line)}
                    />
                  </View>
                );
              })}
            </View>
          </ScrollView>
      
          {/* 🔍 EXPANDED ANALYTICS STRIP */}
          {expandedLine?.line != null && (
            <View style={styles.analytics}>
              {loadingAnalytics && (
                <Text style={styles.analyticsText}>
                  Loading analytics…
                </Text>
              )}

              {!loadingAnalytics && analytics && (
                <>
                  <Text style={styles.analyticsText}>
                    Fair odds: {analytics.fair_odds ?? "—"}
                  </Text>

                  <Text style={styles.analyticsText}>
                    On pace for:{" "}
                    {analytics.on_pace_value != null
                      ? analytics.on_pace_value.toFixed(1)
                      : "—"}{" "}
                    {market}
                  </Text>

                  <Text style={styles.analyticsText}>
                    Δ vs pace:{" "}
                    {analytics.delta_vs_pace != null
                      ? analytics.delta_vs_pace.toFixed(1)
                      : "—"}
                  </Text>

                  <Text style={styles.analyticsText}>
                    L5:{" "}
                    {analytics.hit_rate_l5 != null
                      ? `${Math.round(analytics.hit_rate_l5 * 100)}%`
                      : "—"}{" "}
                    · L10:{" "}
                    {analytics.hit_rate_l10 != null
                      ? `${Math.round(analytics.hit_rate_l10 * 100)}%`
                      : "—"}
                  </Text>

                  {analytics.blowout_flag && (
                    <Text style={styles.analyticsText}>
                      ⚠ Blowout risk
                    </Text>
                  )}
                </>
              )}

              {!loadingAnalytics && !analytics && (
                <Text style={styles.analyticsText}>
                  No analytics available
                </Text>
              )}
            </View>
          )}
        </View>
      )}
    </View>
  );
}

const styles = StyleSheet.create({
  label: {
    fontSize: 11,
    fontWeight: "700",
    marginBottom: 4,
  },
  row: {
    flexDirection: "row",
    gap: 8,
    flexWrap: "nowrap", // IMPORTANT
  },
  analytics: {
    marginTop: 6,
    padding: 8,
    borderRadius: 8,
    backgroundColor: "#00000011",
  },
  analyticsText: {
    fontSize: 11,
    opacity: 0.85,
  },
});