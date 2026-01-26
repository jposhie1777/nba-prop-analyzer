import { View, Text, StyleSheet, ScrollView } from "react-native";
import { useMemo } from "react";
import { useTheme } from "@/store/useTheme";
import { useParlayTracker } from "@/store/useParlayTracker";

function formatTime(ts: string) {
  const d = new Date(ts);
  return d.toLocaleTimeString([], {
    hour: "numeric",
    minute: "2-digit",
  });
}

export function TrackedParlayList() {
  const { colors } = useTheme();
  const { tracked } = useParlayTracker();

  const parlays = useMemo(
    () =>
      Object.values(tracked).sort(
        (a, b) =>
          new Date(b.created_at).getTime() -
          new Date(a.created_at).getTime()
      ),
    [tracked]
  );

  if (!parlays.length) {
    return (
      <View style={styles.empty}>
        <Text style={{ color: colors.text.muted }}>
          No tracked parlays yet
        </Text>
      </View>
    );
  }

  return (
    <ScrollView
      contentContainerStyle={styles.list}
      showsVerticalScrollIndicator={false}
    >
      {parlays.map((p) => (
        <View
          key={p.parlay_id}
          style={[
            styles.card,
            {
              backgroundColor: colors.surface.card,
              borderColor: colors.border.subtle,
            },
          ]}
        >
          {/* Header */}
          <View style={styles.header}>
            <Text
              style={[
                styles.title,
                { color: colors.text.primary },
              ]}
            >
              {p.legs.length}-Leg Parlay
            </Text>

            <Text
              style={{
                color: colors.text.muted,
                fontSize: 12,
                fontWeight: "700",
              }}
            >
              {formatTime(p.created_at)}
            </Text>
          </View>

          {/* Odds */}
          <Text
            style={{
              color: colors.text.secondary,
              fontWeight: "800",
              marginTop: 2,
            }}
          >
            ${p.stake} →{" "}
            {p.payout ? `$${p.payout.toFixed(2)}` : "—"}
          </Text>

          {/* Legs */}
          <View style={{ marginTop: 6 }}>
            {p.legs.map((l) => (
              <Text
                key={l.leg_id}
                style={{
                  color: colors.text.primary,
                  fontSize: 12,
                  fontWeight: "700",
                }}
              >
                • {l.player_name} {l.market}{" "}
                {l.side === "under" ? "U" : "O"} {l.line}
              </Text>
            ))}
          </View>

          {/* Source */}
          <Text
            style={{
              marginTop: 6,
              fontSize: 11,
              fontWeight: "800",
              color: colors.text.muted,
            }}
          >
            Tracked via: {p.source}
          </Text>
        </View>
      ))}
    </ScrollView>
  );
}

const styles = StyleSheet.create({
  list: {
    padding: 12,
    gap: 10,
  },

  card: {
    borderWidth: 1,
    borderRadius: 16,
    padding: 12,
  },

  header: {
    flexDirection: "row",
    justifyContent: "space-between",
    alignItems: "center",
  },

  title: {
    fontSize: 14,
    fontWeight: "900",
  },

  empty: {
    padding: 24,
    alignItems: "center",
  },
});
