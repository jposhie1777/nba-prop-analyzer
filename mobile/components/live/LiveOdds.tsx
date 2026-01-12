// components/live/LiveOdds.tsx
import { View, Text, StyleSheet, ScrollView } from "react-native";
import { useTheme } from "@/store/useTheme";
import { PlayerPropMarket } from "@/types/betting";
import { LinePill } from "./props/LinePill";

type PlayerBlock = {
  player_id: number;
  name: string;
  markets: PlayerPropMarket[];
};

type Props = {
  players: PlayerBlock[];
};

export function LiveOdds({ players }: Props) {
  const { colors } = useTheme();

  if (!players.length) {
    return (
      <Text style={{ color: colors.text.muted, marginTop: 8 }}>
        No live props available
      </Text>
    );
  }

  function handlePress(selection: any) {
    // 🔜 Next step: save bet
    console.log("BET PRESS", selection.selectionId);
  }

  return (
    <View style={styles.wrap}>
      {players.map((player) => (
        <View
          key={player.player_id}
          style={[
            styles.playerBlock,
            { borderColor: colors.border.subtle },
          ]}
        >
          {/* PLAYER NAME */}
          <Text
            style={[
              styles.playerName,
              { color: colors.text.primary },
            ]}
          >
            {player.name}
          </Text>

          {/* MARKETS */}
          {player.markets.map((market) => (
            <View key={market.marketKey} style={styles.marketBlock}>
              <Text
                style={[
                  styles.marketLabel,
                  { color: colors.text.secondary },
                ]}
              >
                {market.marketKey}
              </Text>

              <ScrollView
                horizontal
                showsHorizontalScrollIndicator={false}
                contentContainerStyle={styles.rail}
              >
                {market.selections.map((sel) => (
                  <LinePill
                    key={sel.selectionId}
                    selection={sel}
                    onPress={handlePress}
                  />
                ))}
              </ScrollView>
            </View>
          ))}
        </View>
      ))}
    </View>
  );
}

const styles = StyleSheet.create({
  wrap: {
    marginTop: 12,
    gap: 12,
  },
  playerBlock: {
    borderWidth: 1,
    borderRadius: 12,
    padding: 10,
    gap: 8,
  },
  playerName: {
    fontSize: 13,
    fontWeight: "800",
  },
  marketBlock: {
    gap: 4,
  },
  marketLabel: {
    fontSize: 12,
    fontWeight: "700",
  },
  rail: {
    gap: 8,
    paddingVertical: 2,
  },
});