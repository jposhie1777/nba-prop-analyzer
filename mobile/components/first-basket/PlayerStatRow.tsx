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

  if (!side) return null;

  return (
    <View
      style={{
        flexDirection: "row",
        alignItems: "center",
        paddingVertical: 8,
        paddingHorizontal: 10,
        borderRadius: 10,
        backgroundColor: highlight
          ? `${colors.accent.primary}22`
          : "transparent",
        marginBottom: 4,
      }}
    >
      {/* Player */}
      <Text
        numberOfLines={1}
        style={{
          flex: 2,
          fontSize: 14,
          fontWeight: highlight ? "700" : "500",
          color: colors.text.primary,
        }}
      >
        {side.player}
      </Text>

      {/* FB% */}
      <Stat value={`${(side.firstBasketPct * 100).toFixed(1)}%`} />

      {/* Shot% */}
      <Stat value={`${(side.firstShotShare * 100).toFixed(0)}%`} />

      {/* FB */}
      <Stat value={side.playerFirstBasketCount} />

      {/* Team FB */}
      <Stat value={side.playerTeamFirstBasketCount} />
    </View>
  );
}

function Stat({ value }: { value: string | number }) {
  const { colors } = useTheme();

  return (
    <View style={{ flex: 1, alignItems: "center" }}>
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