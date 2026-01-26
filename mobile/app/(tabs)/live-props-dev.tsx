// /app/(tabs)/live-props-dev.tsx
import { View, Text, FlatList, StyleSheet } from "react-native";
import { useMemo, useState } from "react";

import { useTheme } from "@/store/useTheme";
import LivePropCard from "@/components/live/LivePropCard";
import { LivePropFilterBar } from "@/components/live/LivePropFilterBar";

// 🔁 Cursor-based pagination hook
import { useLivePropsInfinite } from "@/hooks/useLivePropsInfinite";

/* ======================================================
   TYPES
====================================================== */

type MarketType = "OVER" | "UNDER" | "MILESTONE";

type Filters = {
  stats: string[];           // [] = ALL
  marketTypes: MarketType[]; // [] = ALL
  minOdds: number;
  maxOdds: number;
};

/* ======================================================
   CONFIG
====================================================== */

// Safety cap so mobile never explodes
const MAX_ROWS = 600;

const DEFAULT_FILTERS: Filters = {
  stats: [],
  marketTypes: [],
  minOdds: -800,
  maxOdds: 400,
};

/* ======================================================
   FILTER + SORT
====================================================== */

function applyFiltersAndSort(data: any[], filters: Filters) {
  return data
    .filter((item) => {
      // ---- STAT FILTER ----
      if (
        filters.stats.length > 0 &&
        !filters.stats.includes(item.market)
      ) {
        return false;
      }

      // ---- MARKET TYPE FILTER ----
      if (
        filters.marketTypes.length > 0 &&
        !filters.marketTypes.includes(item.display_odds_side)
      ) {
        return false;
      }

      // ---- ODDS FILTER ----
      if (item.display_odds == null) return false;

      if (
        item.display_odds < filters.minOdds ||
        item.display_odds > filters.maxOdds
      ) {
        return false;
      }

      return true;
    })
    // ---- DEFAULT SORT: LOW → HIGH ODDS ----
    .sort((a, b) => a.display_odds - b.display_odds);
}

/* ======================================================
   SCREEN
====================================================== */

export default function LivePropsDevScreen() {
  const { colors } = useTheme();
  const [filters, setFilters] = useState<Filters>(DEFAULT_FILTERS);

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
     GUARDRAIL + FILTER + SORT
  ---------------------------------- */
  const visibleRows = useMemo(() => {
    const capped =
      allRows.length <= MAX_ROWS
        ? allRows
        : allRows.slice(0, MAX_ROWS);

    return applyFiltersAndSort(capped, filters);
  }, [allRows, filters]);

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

  if (!visibleRows || visibleRows.length === 0) {
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
      {/* FILTER BAR */}
      <LivePropFilterBar
        filters={filters}
        setFilters={setFilters}
      />

      {/* LIST */}
      <FlatList
        data={visibleRows}
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