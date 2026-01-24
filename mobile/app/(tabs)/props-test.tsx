// app/(tabs)/props-test
import {
  View,
  Text,
  StyleSheet,
  FlatList,
  Pressable,
} from "react-native";
import { useMemo, useState, useCallback, useRef, useEffect } from "react";
import Slider from "@react-native-community/slider";
import AsyncStorage from "@react-native-async-storage/async-storage";
import { GestureHandlerRootView } from "react-native-gesture-handler";

import PropCard from "@/components/PropCard";
import { PropBetslipDrawer } from "@/components/prop/PropBetslipDrawer";
import { useTheme } from "@/store/useTheme";
import { useSavedBets } from "@/store/useSavedBets";
import { useHistoricalPlayerTrends } from "@/hooks/useHistoricalPlayerTrends";
import { resolveSparklineByMarket } from "@/utils/resolveSparkline";
import { usePlayerPropsMaster } from "@/hooks/usePlayerPropsMaster";

/* ======================================================
   Constants
====================================================== */
const FILTERS_KEY = "props_test_filters_v1";
const PAGE_SIZE = 200;

/* ======================================================
   Types
====================================================== */
type GroupedProp = any & {
  books?: {
    bookmaker: string;
    odds: number;
  }[];
};

/* ======================================================
   Screen
====================================================== */
export default function PropsTestScreen() {
  const colors = useTheme((s) => s.colors);
  const styles = useMemo(() => makeStyles(colors), [colors]);

  const savedIds = useSavedBets((s) => s.savedIds);
  const toggleSave = useSavedBets((s) => s.toggleSave);

  const {
    props: rawProps,
    loading,
    filters,
    setFilters,
    fetchNext, // ⬅️ REQUIRED from hook
  } = usePlayerPropsMaster({
    limit: PAGE_SIZE,
  });

  const { getByPlayer } = useHistoricalPlayerTrends();

  const listRef = useRef<FlatList>(null);
  const [expandedId, setExpandedId] = useState<string | null>(null);

  const [offset, setOffset] = useState(0);
  const [loadingMore, setLoadingMore] = useState(false);
  const [hasMore, setHasMore] = useState(true);

  const toggleExpand = useCallback((id: string) => {
    setExpandedId((prev) => (prev === id ? null : id));
  }, []);

  /* ======================================================
     FILTER PERSISTENCE
  ====================================================== */
  useEffect(() => {
    (async () => {
      const raw = await AsyncStorage.getItem(FILTERS_KEY);
      if (!raw) return;
      setFilters((f) => ({ ...f, ...JSON.parse(raw) }));
    })();
  }, [setFilters]);

  useEffect(() => {
    AsyncStorage.setItem(FILTERS_KEY, JSON.stringify(filters));
  }, [filters]);

  /* ======================================================
     PAGINATION
  ====================================================== */
  const loadMore = useCallback(async () => {
    if (loadingMore || !hasMore) return;

    setLoadingMore(true);
    const count = await fetchNext({ offset });

    setOffset((o) => o + PAGE_SIZE);
    if (count < PAGE_SIZE) setHasMore(false);

    setLoadingMore(false);
  }, [fetchNext, offset, loadingMore, hasMore]);

  /* ======================================================
     MULTI-BOOK GROUPING
  ====================================================== */
  const props = useMemo(() => {
    const base = rawProps.filter((p) => {
      if (!p.player) return false;
      if (!p.market) return false;
      if (p.line == null) return false;
      return true;
    });

    const map = new Map<string, GroupedProp>();

    base.forEach((p) => {
      const key = `${p.player}-${p.market}-${p.line}`;

      if (!map.has(key)) {
        map.set(key, {
          ...p,
          id: key,
          books: [],
        });
      }

      if (p.bookmaker) {
        map.get(key)!.books!.push({
          bookmaker: p.bookmaker,
          odds: p.odds,
        });
      }
    });

    return Array.from(map.values());
  }, [rawProps]);

  /* ======================================================
     RENDER ITEM
  ====================================================== */
  const renderItem = useCallback(
    ({ item }: { item: GroupedProp }) => {
      const trend = getByPlayer(item.player);
      const spark = resolveSparklineByMarket(item.market, trend);

      return (
        <PropCard
          {...item}
          books={item.books}
          scrollRef={listRef}
          saved={savedIds.has(item.id)}
          onToggleSave={() => toggleSave(item.id)}
          expanded={expandedId === item.id}
          onToggleExpand={() => toggleExpand(item.id)}
          sparkline_l5={spark?.sparkline_l5}
          sparkline_l10={spark?.sparkline_l10}
          sparkline_l20={spark?.sparkline_l20}
          last5_dates={trend?.last5_dates}
          last10_dates={trend?.last10_dates}
          last20_dates={trend?.last20_dates}
        />
      );
    },
    [savedIds, expandedId, toggleSave, toggleExpand, getByPlayer]
  );

  if (loading) {
    return (
      <GestureHandlerRootView style={styles.root}>
        <View style={styles.center}>
          <Text style={styles.loading}>Loading test props…</Text>
        </View>
      </GestureHandlerRootView>
    );
  }

  /* ======================================================
     UI
  ====================================================== */
  return (
    <GestureHandlerRootView style={styles.root}>
      <View style={styles.screen}>
        {/* ================= FILTERS ================= */}
        <View style={styles.filters}>
          <Text style={styles.filtersTitle}>Filters</Text>

          <View style={styles.pills}>
            {filters.markets.map((mkt) => {
              const active = filters.market === mkt;
              return (
                <Pressable
                  key={mkt}
                  onPress={() =>
                    setFilters((f) => ({
                      ...f,
                      market: active ? "ALL" : mkt,
                    }))
                  }
                >
                  <Text
                    style={[
                      styles.pill,
                      active && styles.pillActive,
                    ]}
                  >
                    {mkt}
                  </Text>
                </Pressable>
              );
            })}
          </View>

          <Text style={styles.sliderLabel}>
            Hit Rate ≥ {filters.minHitRate}%
          </Text>
          <Slider
            minimumValue={0}
            maximumValue={100}
            step={5}
            value={filters.minHitRate}
            onValueChange={(v) =>
              setFilters((f) => ({ ...f, minHitRate: v }))
            }
            minimumTrackTintColor={colors.accent.primary}
            thumbTintColor={colors.accent.primary}
          />
        </View>

        {/* ================= LIST ================= */}
        <FlatList
          ref={listRef}
          data={props}
          keyExtractor={(item) => item.id}
          renderItem={renderItem}
          showsVerticalScrollIndicator={false}
          onEndReached={loadMore}
          onEndReachedThreshold={0.6}
          ListFooterComponent={
            loadingMore ? (
              <Text style={{ textAlign: "center", padding: 16 }}>
                Loading more…
              </Text>
            ) : null
          }
        />

        <PropBetslipDrawer />
      </View>
    </GestureHandlerRootView>
  );
}

/* ======================================================
   Styles
====================================================== */
function makeStyles(colors: any) {
  return StyleSheet.create({
    root: { flex: 1 },
    screen: {
      flex: 1,
      backgroundColor: colors.surface.screen,
    },
    center: {
      flex: 1,
      alignItems: "center",
      justifyContent: "center",
    },
    loading: {
      color: colors.text.muted,
    },
    filters: {
      padding: 12,
      borderBottomWidth: StyleSheet.hairlineWidth,
      borderBottomColor: colors.border.subtle,
    },
    filtersTitle: {
      fontSize: 14,
      fontWeight: "800",
      color: colors.text.primary,
      marginBottom: 8,
    },
    pills: {
      flexDirection: "row",
      flexWrap: "wrap",
      marginBottom: 8,
      gap: 6,
    },
    pill: {
      paddingHorizontal: 10,
      paddingVertical: 6,
      borderRadius: 999,
      borderWidth: 1,
      borderColor: colors.border.subtle,
      color: colors.text.muted,
      fontSize: 12,
    },
    pillActive: {
      backgroundColor: colors.accent.primary,
      color: colors.text.inverse,
      borderColor: colors.accent.primary,
    },
    sliderLabel: {
      fontSize: 12,
      color: colors.text.muted,
      marginTop: 6,
    },
  });
}