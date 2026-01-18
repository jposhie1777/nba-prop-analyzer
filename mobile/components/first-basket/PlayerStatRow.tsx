// mobile/components/first-basket/PlayerStatRow.tsx
import React from "react";
import { View, Text } from "react-native";
import { useTheme } from "@/store/useTheme";
import type { FirstBasketSide } from "@/hooks/useFirstBasketMatchups";


type Side = {
  player: string;
  firstBasketPct: number;
  shotShare: number;
  firstBasketCount: number;
  teamFirstBasketCount: number;
};

export function PlayerStatRow({
  side,
}: {
  side: FirstBasketSide | null;
}) {
  const { colors } = useTheme();

  if (!side) {
    return (
      <View style={{ flexDirection: "row", opacity: 0.4 }}>
        <Text style={{ flex: 2 }}>—</Text>
        <Text style={{ flex: 1, textAlign: "right" }}>—</Text>
        <Text style={{ flex: 1, textAlign: "right" }}>—</Text>
        <Text style={{ flex: 1, textAlign: "right" }}>—</Text>
        <Text style={{ flex: 1, textAlign: "right" }}>—</Text>
      </View>
    );
  }

  return (
    <View style={{ flexDirection: "row" }}>
      <Text style={{ flex: 2 }} numberOfLines={1}>
        {side.player}
      </Text>

      <Text style={{ flex: 1, textAlign: "right" }}>
        {(side.firstBasketPct * 100).toFixed(1)}%
      </Text>

      <Text style={{ flex: 1, textAlign: "right" }}>
        {(side.firstShotShare * 100).toFixed(0)}%
      </Text>

      <Text style={{ flex: 1, textAlign: "right" }}>
        {side.playerFirstBasketCount}
      </Text>

      <Text style={{ flex: 1, textAlign: "right" }}>
        {side.playerTeamFirstBasketCount}
      </Text>
    </View>
  );
}
