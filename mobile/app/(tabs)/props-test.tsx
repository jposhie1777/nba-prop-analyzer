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
import { GestureHandlerRootView } from "react-native-gesture-handler";

import PropCard from "@/components/PropCard";
import { useTheme } from "@/store/useTheme";
import { useSavedBets } from "@/store/useSavedBets";
import { useHistoricalPlayerTrends } from "@/hooks/useHistoricalPlayerTrends";
import { resolveSparklineByMarket } from "@/utils/resolveSparkline";
import { usePlayerPropsMaster } from "@/hooks/usePlayerPropsMaster";

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
  } = usePlayerPropsMaster();

  const { getByPlayer } = useHistoricalPlayerTrends();

  const listRef = useRef<FlatList>(null);
  const [expandedId, setExpandedId] = useState<string | null>(null);

  const toggleExpand = useCallback((id: string) => {
    setExpandedId((prev) => (prev === id ? null : id));
  }, []);

  /* ======================================================
     DEBUG — RAW INPUT
  ====================================================== */
  useEffect(() => {
    console.log("🧪 [SCREEN] rawProps length:", rawProps.length);
    if (rawProps.length) {
      console.log("🧪 [SCREEN] sample raw prop:", rawProps[0]);
    }
  }, [rawProps]);

  /* ======================================================
     SANITIZE + FILTER + SORT
  ====================================================== */
  const props = useMemo(() => {
    const cleaned = rawProps
      .filter((p) => {
        if (!p.player) return false;
        if (!p.market) return false;
        if (p.line == null) return false;
        if (!p.id) return false;

        // ➕ ADD: market window filter (unchecked = ALL)
        if (
          filters.marketWindow &&
          p.market_window !== filters.marketWindow
        ) {
          return false;
        }

        // ➕ ADD: hit rate filter by selected window
        const hitRate =
          filters.hitRateWindow === "L5"
            ? p.hit_rate_l5
            : filters.hitRateWindow === "L10"
            ? p.hit_rate_l10
            : p.hit_rate_l20;

        if (hitRate != null && hitRate < filters.minHitRate) {
          return false;
        }

        // ➕ ADD: odds filter
        if (p.odds < filters.minOdds) return false;
        if (p.odds > filters.maxOdds) return false;

        return true;
      })
      .map((p, idx) => ({
        ...p,
        id: `${p.id}::${idx}`,
      }));

    // ➕ ADD: default sorting
    cleaned.sort((a, b) => {
      const getHR = (p: any) =>
        filters.hitRateWindow === "L5"
          ? p.hit_rate_l5 ?? 0
          : filters.hitRateWindow === "L10"
          ? p.hit_rate_l10 ?? 0
          : p.hit_rate_l20 ?? 0;

      const hrDiff = getHR(b) - getHR(a);
      if (hrDiff !== 0) return hrDiff;

      // Secondary: odds ASC
      return (a.odds ?? 0) - (b.odds ?? 0);
    });

    console.log("🧪 [SCREEN] props after sanitize/filter/sort:", cleaned.length);

    return cleaned;
  }, [rawProps, filters]);

  /* ======================================================
     RENDER ITEM
  ====================================================== */
  const renderItem = useCallback(
    ({ item }: any) => {
      const trend = getByPlayer(item.player);
      const spark = resolveSparklineByMarket(item.market, trend);

      if (!spark) {
        console.warn(
          "⚠️ Unhandled sparkline market:",
          item.market
        );
      }

      return (
        <PropCard
          {...item}
          playerId={item.player_id}
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
        <View style={styles.filters}>
          <Text style={styles.filtersTitle}>Filters</Text>

          {/* MARKETS */}
          <View style={styles.pills}>
            {filters.markets.map((mkt) => {
              const active = filters.market === mkt;
              return (
                <Pressable
                  key={`mkt-${mkt}`}
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

          {/* ➕ ADD: MARKET WINDOW */}
          <View style={styles.pills}>
            {(["FULL", "Q1", "FIRST3MIN"] as const).map((w) => {
              const active = filters.marketWindow === w;
              return (
                <Pressable
                  key={`mw-${w}`}
                  onPress={() =>
                    setFilters((f) => ({
                      ...f,
                      marketWindow: active ? null : w,
                    }))
                  }
                >
                  <Text
                    style={[
                      styles.pill,
                      active && styles.pillActive,
                    ]}
                  >
                    {w}
                  </Text>
                </Pressable>
              );
            })}
          </View>

          {/* HIT RATE WINDOW */}
          <View style={styles.pills}>
            {(["L5", "L10", "L20"] as const).map((w) => {
              const active = filters.hitRateWindow === w;
              return (
                <Pressable
                  key={`hr-${w}`}
                  onPress={() =>
                    setFilters((f) => ({
                      ...f,
                      hitRateWindow: w,
                    }))
                  }
                >
                  <Text
                    style={[
                      styles.pill,
                      active && styles.pillActive,
                    ]}
                  >
                    {w}
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

          <Text style={styles.sliderLabel}>
            Odds {filters.minOdds} → {filters.maxOdds}
          </Text>
          <Slider
            minimumValue={-1000}
            maximumValue={1000}
            step={25}
            value={filters.minOdds}
            onValueChange={(v) =>
              setFilters((f) => ({ ...f, minOdds: v }))
            }
          />
          <Slider
            minimumValue={-1000}
            maximumValue={1000}
            step={25}
            value={filters.maxOdds}
            onValueChange={(v) =>
              setFilters((f) => ({ ...f, maxOdds: v }))
            }
          />
        </View>

        <Text style={{ padding: 8, color: "red" }}>
          PROPS COUNT: {props.length}
        </Text>

        <FlatList
          ref={listRef}
          data={props}
          keyExtractor={(item) => item.id}
          renderItem={renderItem}
          showsVerticalScrollIndicator={false}
        />
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