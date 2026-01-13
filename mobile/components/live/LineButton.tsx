import { Pressable, Text, StyleSheet } from "react-native";
import { useTheme } from "@/store/useTheme";

export function LineButton({ line, market }: any) {
  const { colors } = useTheme();

  return (
    <Pressable
      style={[
        styles.btn,
        { backgroundColor: colors.surface.card },
      ]}
      onPress={() => {
        // TODO: save bet
        console.log("SAVE", market, line.line);
      }}
    >
      <Text style={styles.line}>{line.line}</Text>
      <Text style={styles.odds}>
        O {line.books.draftkings?.over}
      </Text>
      <Text style={styles.odds}>
        U {line.books.draftkings?.under}
      </Text>
    </Pressable>
  );
}

const styles = StyleSheet.create({
  btn: {
    padding: 8,
    borderRadius: 10,
    minWidth: 72,
    alignItems: "center",
  },
  line: {
    fontWeight: "800",
    fontSize: 12,
  },
  odds: {
    fontSize: 10,
  },
});