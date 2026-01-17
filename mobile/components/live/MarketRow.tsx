// components/live/marketrow
import React, { useRef, useEffect } from "react";
import { View, Text, ScrollView, StyleSheet } from "react-native";
import { LineButton } from "./LineButton";
import { OverUnderButton } from "./OverUnderButton";
import { useSavedBets } from "@/store/useSavedBets";

export function MarketRow({ market, lines, current }: any) {
  const { toggleSave, savedIds } = useSavedBets();
  const scrollRef = useRef<ScrollView>(null);
  const buttonWidthRef = useRef<number>(0);
  const overUnderByLine = new Map<number, any>();
  const didAutoScroll = useRef(false);

  for (const l of lines) {
    if (l.line_type !== "over_under") continue;
  
    const existing = overUnderByLine.get(l.line);
    if (
      !existing ||
      new Date(l.snapshot_ts).getTime() >
        new Date(existing.snapshot_ts).getTime()
    ) {
      overUnderByLine.set(l.line, l);
    }
  }
  
  const mainLine = Array.from(overUnderByLine.values())
    .find(
      (l) => l.over_odds != null || l.under_odds != null
    );

  const getState = (lineValue: number) => {
    const remaining = lineValue - current;
    if (remaining <= 0) return "hit";
    if (remaining === 1) return "close";
    return "pending";
  };

  const getMilestoneOdds = (line: any): number | null => {
    const raw =
      line.price ??
      line.over_odds ??
      line.books?.draftkings?.milestone;
  
    if (raw === null || raw === undefined) return null;
    return Number(raw);
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
    if (!mainLine) return "";
    return `ou:${mainLine.game_id}:${mainLine.player_id}:${market}:${side}:${line}`;
  };
  
  const getMilestoneBetId = (lineValue: number) => {
    if (!mainLine) return "";
    return `ms:${mainLine.game_id}:${mainLine.player_id}:${market}:${lineValue}`;
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

  return (
    <View>
      <Text style={styles.label}>{market}</Text>

      {/* MAIN OVER / UNDER */}
      {mainLine && (
        <View style={{ marginBottom: 6 }}>
          <View style={{ flexDirection: "row", gap: 10 }}>
            {(() => {
              const overBetId = getOverUnderBetId("over", mainLine.line);
              const underBetId = getOverUnderBetId("under", mainLine.line);
      
              return (
                <>
                  <OverUnderButton
                    side="over"
                    line={mainLine.line}
                    odds={mainLine.over_odds}
                    disabled={mainLine.over_odds == null}
                    isSelected={savedIds.has(overBetId)}
                    onPress={() => {
                      if (mainLine.over_odds == null) return;
                      toggleSave({
                        id: overBetId,
                        playerId: mainLine.player_id,
                        gameId: mainLine.game_id,
                        market,
                        line: mainLine.line,
                        side: "over",
                        odds: mainLine.over_odds,
                      });
                    }}
                  />
      
                  <OverUnderButton
                    side="under"
                    line={mainLine.line}
                    odds={mainLine.under_odds}
                    disabled={mainLine.under_odds == null}
                    isSelected={savedIds.has(underBetId)}
                    onPress={() => {
                      if (mainLine.under_odds == null) return;
                      toggleSave({
                        id: underBetId,
                        playerId: mainLine.player_id,
                        gameId: mainLine.game_id,
                        market,
                        line: mainLine.line,
                        side: "under",
                        odds: mainLine.under_odds,
                      });
                    }}
                  />
                </>
              );
            })()}
          </View>
        </View>
      )}

      {/* MILESTONES */}
      {milestones.length > 0 && (
        <ScrollView
          ref={scrollRef}
          horizontal
          showsHorizontalScrollIndicator={false}
          contentContainerStyle={{ paddingRight: 12 }}
          style={{ marginTop: 6 }}
        >
          <View style={styles.row}>
            {milestones.map((m: any, idx: number) => (
              <View
                key={`ms-${m.line}`}
                onLayout={
                  idx === 0
                    ? (e) => {
                        buttonWidthRef.current =
                          e.nativeEvent.layout.width;
                      }
                    : undefined
                }
              >
                
                {(() => {
                  const betId = getMilestoneBetId(m.line);
                  const isSelected = savedIds.has(betId);
                
                  return (
                    <LineButton
                      line={m}
                      market={market}
                      state={getState(m.line)}
                      isSelected={isSelected}
                      onPress={() => {
                        toggleSave({
                          id: betId,
                          playerId: m.player_id,
                          gameId: m.game_id,
                          market,
                          line: m.line,
                          side: "milestone",
                          odds: getMilestoneOdds(m),
                        });
                      }}
                    />
                  );
                })()}
              </View>
            ))}
          </View>
        </ScrollView>
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
});