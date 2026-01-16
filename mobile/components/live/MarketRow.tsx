// components/live/marketrow
import { View, Text, ScrollView, StyleSheet } from "react-native";
import { LineButton } from "./LineButton";
import { OverUnderButton } from "./OverUnderButton";
import { useBetsStore } from "@/store/useSavedBets";

export function MarketRow({ market, lines, current }: any) {
  // -----------------------------
  // Split main vs milestone
  // -----------------------------
  const { toggleSave } = useSavedBets();

  const mainLine = lines.find(
    (l: any) => l.line_type === "over_under"
  );
  
  const getState = (lineValue: number) => {
    const remaining = lineValue - current;
  
    if (remaining <= 0) return "hit";     // ✅ already hit
    if (remaining === 1) return "close";  // 🟡 one away
    return "pending";                     // ⏳ still needs work
  };

  const milestones = lines
    .filter(
      (l: any) =>
        l.line_type === "milestone" &&
        l.line > current               // 👈 ONLY FUTURE MILESTONES
    )
    .sort((a: any, b: any) => a.line - b.line)
    .slice(0, 3);

  if (!mainLine && milestones.length === 0) {
    return null;
  }

  return (
    <View>
      <Text style={styles.label}>{market}</Text>

      {/* -----------------------------
          MAIN LINE (unchanged UI)
      ----------------------------- */}
      {mainLine && (
        <View style={{ marginBottom: 6 }}>
          <View style={{ flexDirection: "row", gap: 10 }}>
            <OverUnderButton
              side="over"
              line={mainLine.line}
              odds={mainLine.over_odds}
              disabled={mainLine.over_odds == null}
              onPress={() => {
                if (mainLine.over_odds == null) return;
                addBet({
                  game_id: mainLine.game_id,
                  player_id: mainLine.player_id,
                  player_name: mainLine.player_name,
                  market,
                  side: "over",
                  line: mainLine.line,
                  odds: mainLine.over_odds,
                  book: mainLine.book,
                  snapshot_ts: mainLine.snapshot_ts,
                });
              }}
            />
      
            <OverUnderButton
              side="under"
              line={mainLine.line}
              odds={mainLine.under_odds}
              disabled={mainLine.under_odds == null}
              onPress={() => {
                if (mainLine.under_odds == null) return;
                addBet({
                  game_id: mainLine.game_id,
                  player_id: mainLine.player_id,
                  player_name: mainLine.player_name, // 👈 add this for parity
                  market,
                  side: "under",
                  line: mainLine.line,
                  odds: mainLine.under_odds,
                  book: mainLine.book,
                  snapshot_ts: mainLine.snapshot_ts,
                });
              }}
            />
          </View>
        </View>
      )}

      {/* -----------------------------
          MILESTONE LADDER (NEW)
      ----------------------------- */}
      {milestones.length > 0 && (
        <ScrollView
          horizontal
          showsHorizontalScrollIndicator={false}
          style={{ marginTop: 6 }}
        >
          <View style={styles.row}>
            {milestones.map((m: any) => (
              <LineButton
                key={`ms-${m.line}`}
                line={m}
                market={market}
                state={getState(m.line)}
              />
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
  },
});