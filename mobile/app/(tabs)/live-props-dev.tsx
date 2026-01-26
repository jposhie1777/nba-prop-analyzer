// /app/(tabs)/live-props-dev.tsx
import { View, Text, FlatList, StyleSheet } from "react-native";
import { useMemo } from "react";

import { useTheme } from "@/store/useTheme";
import LivePropCard from "@/components/live/LivePropCard";

// 🔁 NEW HOOK (cursor-based pagination)
import { useLivePropsInfinite } from "@/hooks/useLivePropsInfinite";

/* ======================================================
   CONFIG
====================================================== */

// Safety cap so mobile never explodes
const MAX_ROWS = 600;

/* ======================================================
   SCREEN
====================================================== */

export default function LivePropsDevScreen() {
  const { colors } = useTheme();

  const {
    data,
    error,
    isLoading,
    fetchNextPage,
    hasNextPage,
    isFetchingNextPage,
  } = useLivePropsInfinite();

  /* ---------------------------------
     FLATTEN PAGES
  ---------------------------------- */
  const allRows = useMemo(() => {
    return data?.pages.flatMap((p) => p.items) ?? [];
  }, [data]);

  /* ---------------------------------
     GUARDRAIL (OPTIONAL BUT RECOMMENDED)
  ---------------------------------- */
  const trimmedRows = useMemo(() => {
    if (allRows.length <= MAX_ROWS) return allRows;
    return allRows.slice(0, MAX_ROWS);
  }, [allRows]);

  /* ---------------------------------
     STATES
  ---------------------------------- */
  if (isLoading) {
    return (
      <View
        style={[
          styles.center,
          { backgroundColor: colors.surface.screen },
        ]}
      >
        <Text style={{ color: colors.text.muted }}>
          Loading live props…
        </Text>
      </View>
    );
  }

  if (error) {
    return (
      <View
        style={[
          styles.center,
          { backgroundColor: colors.surface.screen },
        ]}
      >
        <Text style={{ color: colors.accent.danger }}>
          Error loading live props
        </Text>
      </View>
    );
  }

  if (!trimmedRows || trimmedRows.length === 0) {
    return (
      <View
        style={[
          styles.center,
          { backgroundColor: colors.surface.screen },
        ]}
      >
        <Text style={{ color: colors.text.muted }}>
          No live props available
        </Text>
      </View>
    );
  }

  /* ---------------------------------
     RENDER
  ---------------------------------- */
  return (
    <View
      style={[
        styles.container,
        { backgroundColor: colors.surface.screen },
      ]}
    >
      <FlatList
        data={trimmedRows}
        keyExtractor={(item) => item.prop_key}
        contentContainerStyle={{ padding: 12 }}
        renderItem={({ item }) => (
          <LivePropCard item={item} />
        )}
        onEndReached={() => {
          if (hasNextPage && !isFetchingNextPage) {
            fetchNextPage();
          }
        }}
        onEndReachedThreshold={0.6}
        ListFooterComponent={
          isFetchingNextPage ? (
            <Text
              style={{
                textAlign: "center",
                paddingVertical: 12,
                color: colors.text.muted,
              }}
            >
              Loading more…
            </Text>
          ) : null
        }
      />
    </View>
  );
}

/* ======================================================
   STYLES
====================================================== */

const styles = StyleSheet.create({
  container: {
    flex: 1,
  },

  center: {
    flex: 1,
    alignItems: "center",
    justifyContent: "center",
  },
});