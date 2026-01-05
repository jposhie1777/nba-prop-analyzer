import { Text, StyleSheet } from "react-native";
import { useTheme } from "@/store/useTheme";
import { LiveGame } from "@/types/live";

export function GameStatus({ game }: { game: LiveGame }) {
  const { colors } = useTheme();

  return (
    <Text style={[styles.text, { color: colors.text.muted }]}>
      {game.period} · {game.clock}
    </Text>
  );
}

const styles = StyleSheet.create({
  text: {
    textAlign: "center",
    fontSize: 12,
  },
});
