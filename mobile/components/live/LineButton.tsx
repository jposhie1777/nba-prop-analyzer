// components/live/LineButton.tsx
import { Pressable, Text, StyleSheet } from "react-native";
import { useTheme } from "@/store/useTheme";

export function LineButton({ line, market, variant }: any) {
  const { colors } = useTheme();

  const isMilestone = line.line_type === "milestone";

  const dk = line.books?.draftkings;

  return (
    <Pressable
      style={[
        styles.btn,
        {
          backgroundColor: colors.surface.card,
          borderColor: isMilestone
            ? colors.accent.primary
            : colors.border.subtle,
        },
      ]}
      onPress={() => {
        console.log(
          "SAVE",
          market,
          line.line,
          isMilestone ? "MILESTONE" : "OU"
        );
      }}
    >
      {/* LINE LABEL */}
      <Text style={styles.line}>
        {isMilestone ? `${line.line}+` : line.line}
      </Text>

      {/* ODDS */}
      {isMilestone ? (
        <Text style={styles.odds}>
          {dk?.milestone ?? "—"}
        </Text>
      ) : (
        <>
          <Text style={styles.odds}>
            O {dk?.over ?? "—"}
          </Text>
          <Text style={styles.odds}>
            U {dk?.under ?? "—"}
          </Text>
        </>
      )}
    </Pressable>
  );
}

const styles = StyleSheet.create({
  btn: {
    padding: 8,
    borderRadius: 10,
    minWidth: 72,
    alignItems: "center",
    borderWidth: 1,
  },
  line: {
    fontWeight: "800",
    fontSize: 12,
  },
  odds: {
    fontSize: 10,
  },
});