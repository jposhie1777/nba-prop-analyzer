// components/live/linebutton
import { Pressable, Text, StyleSheet } from "react-native";
import { useTheme } from "@/store/useTheme";

type Props = {
  line: any;
  market: string;
  playerId: number;
  state?: "hit" | "close" | "pending";
  isSelected?: boolean;
  onSave?: () => void;
  onInspect?: () => void;
};


export function LineButton({
  line,
  market,
  state = "pending",
  isSelected = false,
  onSave,
  onInspect,
}: Props) {
  const { colors } = useTheme();
  const isMilestone = line.line_type === "milestone";

  const milestoneOdds =
    line.price ??
    line.over_odds ??
    line.books?.draftkings?.milestone ??
    "—";

  return (
    <Pressable
      onPress={onSave}
      onLongPress={onInspect}
      delayLongPress={250}
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
        <Text style={styles.odds}>{milestoneOdds}</Text>
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