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
  const overUnderLines = lines.filter(
    (l: any) =>
      l.line_type === "over_under" &&
      (l.over_odds != null || l.under_odds != null)
  );
  const mainLine = overUnderLines
    .sort(
      (a: any, b: any) =>
        new Date(b.snapshot_ts).getTime() -
        new Date(a.snapshot_ts).getTime()
    )[0];

  const getState = (lineValue: number) => {
    const remaining = lineValue - current;
    if (remaining <= 0) return "hit";
    if (remaining === 1) return "close";
    return "pending";
  };

  const milestones = lines
    .filter(
      (l: any) =>
        l.line_type === "milestone" &&
        l.line > current
    )
    .sort((a: any, b: any) => a.line - b.line);

  if (!mainLine && milestones.length === 0) {
    return null;
  }
  
  const getBetId = (side: "over" | "under") => {
    if (!mainLine) return "";
    return `${mainLine.game_id}:${mainLine.player_id}:${market}:${side}:${mainLine.line}`;
  };

  const getMilestoneBetId = (lineValue: number) => {
    if (!mainLine) return "";
    return `${mainLine.game_id}:${mainLine.player_id}:${market}:milestone:${lineValue}`;
  };

  const closeIndex = milestones.findIndex(
    (m: any) => getState(m.line) === "close"
  );
  
  useEffect(() => {
    if (
      closeIndex >= 0 &&
      scrollRef.current &&
      buttonWidthRef.current > 0
    ) {
      scrollRef.current.scrollTo({
        x: closeIndex * (buttonWidthRef.current + 8),
        animated: true,
      });
    }
  }, [closeIndex, milestones]);

  return (
    <View>
      <Text style={styles.label}>{market}</Text>

      {/* MAIN OVER / UNDER */}
      {mainLine && (
        <View style={{ marginBottom: 6 }}>
          <View style={{ flexDirection: "row", gap: 10 }}>
            {(() => {
              const overBetId = getBetId("over");
              const underBetId = getBetId("under");
      
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
                      toggleSave(overBetId);
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
                      toggleSave(underBetId);
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
                        toggleSave(betId);
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