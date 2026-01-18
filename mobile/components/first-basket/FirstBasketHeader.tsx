// mobile/components/first-basket/FirstBasketHeader.tsx
import { View, Text, Image } from "react-native";
import { useTheme } from "@/store/useTheme";

type Props = {
  homeTeam: string;
  awayTeam: string;
  homeLogo?: string;
  awayLogo?: string;
  homeWinPct: number;
  awayWinPct: number;
};

export function FirstBasketHeader({
  homeTeam,
  awayTeam,
  homeLogo,
  awayLogo,
  homeWinPct,
  awayWinPct,
}: Props) {
  const { colors } = useTheme();

  return (
    <View style={{ marginBottom: 12 }}>
      {/* Top Row */}
      <View
        style={{
          flexDirection: "row",
          alignItems: "center",
          justifyContent: "center",
          gap: 12,
        }}
      >
        <Image
          source={{ uri: homeLogo }}
          style={{ width: 28, height: 28 }}
          resizeMode="contain"
        />

        <Text
          style={{
            fontSize: 16,
            fontWeight: "600",
            color: colors.text.primary,
          }}
        >
          {homeTeam} vs {awayTeam}
        </Text>

        <Image
          source={{ uri: awayLogo }}
          style={{ width: 28, height: 28 }}
          resizeMode="contain"
        />
      </View>

      {/* Tip Win Row */}
      <Text
        style={{
          marginTop: 4,
          textAlign: "center",
          fontSize: 12,
          color: colors.text.secondary,
        }}
      >
        Tip Win:{" "}
        <Text style={{ color: colors.accent.primary, fontWeight: "600" }}>
          {homeTeam} {homeWinPct}%
        </Text>
        {" · "}
        <Text style={{ color: colors.text.secondary }}>
          {awayTeam} {awayWinPct}%
        </Text>
      </Text>
    </View>
  );
}
