// components/live/linebutton
import { Pressable, Text, StyleSheet } from "react-native";
import { useTheme } from "@/store/useTheme";

type Props = {
  line: any;
  market: string;
  state?: "hit" | "close" | "pending";
  isSelected?: boolean;
  onPress?: () => void;
};

export function LineButton({
  line,
  market,
  state = "pending",
  isSelected = false,
  onPress,
}: Props) {
  const { colors } = useTheme();
  const isMilestone = line.line_type === "milestone";
  // 🔍 DEBUG: is this LineButton being reused across players?
  console.log("🧩 LINE BUTTON RENDER", {
    market,
    line: line?.line,
    playerId: line?.player_id,
  });

  const milestoneOdds =
    line.price ??
    line.over_odds ??
    line.books?.draftkings?.milestone ??
    "—";

  return (
    <Pressable
      onPress={onPress}
      style={({ pressed }) => [
        styles.btn,
        {
          backgroundColor: isSelected
            ? colors.accent.primary + "22"
            : pressed
            ? colors.surface.subtle
            : colors.surface.card,

          borderColor: isSelected
            ? colors.accent.primary
            : state === "close"
            ? colors.warning
            : state === "hit"
            ? colors.success
            : colors.border.subtle,
        },
      ]}
    >
      <Text
        style={[
          styles.line,
          {
            color: isSelected
              ? colors.accent.primary
              : colors.text.primary,
          },
        ]}
      >
        {isMilestone ? `${line.line}+` : line.line}
      </Text>

      {isMilestone ? (
        <Text style={styles.odds}>
          {milestoneOdds}
        </Text>
      ) : (
        <>
          <Text style={styles.odds}>
            O {line.over_odds ?? "—"}
          </Text>
          <Text style={styles.odds}>
            U {line.under_odds ?? "—"}
          </Text>
        </>
      )}
    </Pressable>
  );
}

const styles = StyleSheet.create({
  btn: {
    paddingVertical: 6,
    paddingHorizontal: 10,
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