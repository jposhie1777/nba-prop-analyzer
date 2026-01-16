import { View, Text, Image } from "react-native";
import { useTheme } from "@/store/useTheme";

export function LineupRow({ player }: { player: any }) {
  const { colors } = useTheme();

  return (
    <View style={{ flexDirection: "row", alignItems: "center", gap: 10 }}>
      <Image
        source={{ uri: player.player_image_url }}
        style={{ width: 28, height: 28, borderRadius: 14 }}
      />

      <Text style={{ color: colors.text.primary, fontSize: 13 }}>
        {player.lineup_position}
      </Text>

      <Text style={{ color: colors.text.secondary, fontSize: 13 }}>
        {player.player}
      </Text>
    </View>
  );
}