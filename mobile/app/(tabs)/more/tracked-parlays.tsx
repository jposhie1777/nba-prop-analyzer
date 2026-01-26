import { View, FlatList, Text, StyleSheet } from "react-native";
import { useMemo } from "react";

import { useTheme } from "@/store/useTheme";
import { useParlayTracker } from "@/store/useParlayTracker";
import TrackedParlayCard from "@/components/tracked/TrackedParlayCard";

export default function TrackedParlaysScreen() {
  const colors = useTheme((s) => s.colors);
  const styles = useMemo(() => createStyles(colors), [colors]);

  // 🔑 Source of truth: Zustand store
  const tracked = useParlayTracker((s) => s.tracked);

  // Normalize → sort (newest first)
  const parlays = useMemo(() => {
    return Object.values(tracked).sort(
      (a, b) =>
        new Date(b.created_at).getTime() -
        new Date(a.created_at).getTime()
    );
  }, [tracked]);

  // Empty state
  if (parlays.length === 0) {
    return (
      <View style={styles.empty}>
        <Text style={styles.emptyText}>
          No tracked parlays yet
        </Text>
      </View>
    );
  }

  return (
    <FlatList
      data={parlays}
      keyExtractor={(item) => item.parlay_id}
      contentContainerStyle={styles.list}
      renderItem={({ item }) => (
        <TrackedParlayCard parlay={item} />
      )}
    />
  );
}

/* ======================================================
   STYLES
====================================================== */

function createStyles(colors: any) {
  return StyleSheet.create({
    list: {
      padding: 12,
    },
    empty: {
      flex: 1,
      alignItems: "center",
      justifyContent: "center",
      padding: 24,
    },
    emptyText: {
      fontSize: 14,
      color: colors.textSecondary,
    },
  });
}
