// app/(tabs)/more/tracked-parlays
import {
  View,
  FlatList,
  Text,
  StyleSheet,
  Pressable,
  Alert,
} from "react-native";
import { useMemo } from "react";

import { useTheme } from "@/store/useTheme";
import { useParlayTracker } from "@/store/useParlayTracker";
import TrackedParlayCard from "@/components/tracked/TrackedParlayCard";

export default function TrackedParlaysScreen() {
  const colors = useTheme((s) => s.colors);
  const styles = useMemo(() => createStyles(colors), [colors]);

  // 🔑 Zustand selectors (separate for perf)
  const tracked = useParlayTracker((s) => s.tracked);
  const clearAll = useParlayTracker((s) => s.clearAll);

  // Normalize → newest first
  const parlays = useMemo(() => {
    return Object.values(tracked).sort(
      (a, b) =>
        new Date(b.created_at).getTime() -
        new Date(a.created_at).getTime()
    );
  }, [tracked]);

  // ---------- EMPTY ----------
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
    <View style={styles.root}>
      {/* ================= HEADER ================= */}
      <View style={styles.header}>
        <Text style={styles.title}>Tracked Parlays</Text>

        <Pressable
          hitSlop={12}
          onPress={() =>
            Alert.alert(
              "Clear all tracked parlays?",
              "This cannot be undone.",
              [
                { text: "Cancel", style: "cancel" },
                {
                  text: "Clear All",
                  style: "destructive",
                  onPress: clearAll,
                },
              ]
            )
          }
        >
          <Text style={styles.clearAll}>Clear All</Text>
        </Pressable>
      </View>

      {/* ================= LIST ================= */}
      <FlatList
        data={parlays}
        keyExtractor={(item) => item.parlay_id}
        contentContainerStyle={styles.list}
        renderItem={({ item }) => (
          <TrackedParlayCard parlay={item} />
        )}
      />
    </View>
  );
}

/* ======================================================
   STYLES
====================================================== */

function createStyles(colors: any) {
  return StyleSheet.create({
    root: {
      flex: 1,
    },

    header: {
      flexDirection: "row",
      alignItems: "center",
      justifyContent: "space-between",
      paddingHorizontal: 16,
      paddingVertical: 12,
      borderBottomWidth: StyleSheet.hairlineWidth,
      borderColor: colors.border.subtle,
      backgroundColor: colors.surface.base,
    },

    title: {
      fontSize: 16,
      fontWeight: "600",
      color: colors.text.primary,
    },

    clearAll: {
      fontSize: 14,
      fontWeight: "600",
      color: colors.accent.danger,
    },

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
      color: colors.text.secondary,
    },
  });
}}
