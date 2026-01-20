// mobile/components/first-basket/PlayerStatRow.tsx
import React from "react";
import { View, Text } from "react-native";
import { useTheme } from "@/store/useTheme";
import type { FirstBasketSide } from "@/hooks/useFirstBasketMatchups";

export function PlayerStatRow({
  side,
  highlight,
}: {
  side: FirstBasketSide;
  highlight?: boolean;
}) {
  const { colors } = useTheme();

  return (
    <View
      style={{
        paddingVertical: 8,
        paddingHorizontal: 10,
        borderRadius: 12,
        backgroundColor: highlight
          ? colors.accent.primary + "22" // 13% opacity
          : "transparent"
        marginBottom: 6,
      }}
    >
      {/* Player name */}
      <Text
        numberOfLines={1}
        style={{
          fontSize: 15,
          fontWeight: highlight ? "700" : "500",
          color: colors.text.primary,
          marginBottom: 4,
        }}
      >
        {side.player}
      </Text>

      {/* Stats grid */}
      <View
        style={{
          flexDirection: "row",
          justifyContent: "space-between",
        }}
      >
        <Stat value={`${(side.firstBasketPct * 100).toFixed(1)}%`} />
        <Stat value={`${(side.firstShotShare * 100).toFixed(0)}%`} />
        <Stat value={side.playerFirstBasketCount} />
        <Stat value={side.playerTeamFirstBasketCount} />
      </View>
    </View>
  );
}

/* ======================================================
   Single Stat Cell
====================================================== */
function Stat({ value }: { value: string | number }) {
  const { colors } = useTheme();

  return (
    <View style={{ alignItems: "center", minWidth: 52 }}>
      <Text
        style={{
          fontSize: 13,
          fontWeight: "600",
          color: colors.text.primary,
        }}
      >
        {value}
      </Text>
    </View>
  );
}