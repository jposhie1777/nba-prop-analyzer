import {
  View,
  Text,
  StyleSheet,
  FlatList,
  Pressable,
} from "react-native";
import { useMemo, useState, useEffect, useCallback, useRef } from "react";
import Slider from "@react-native-community/slider";
import { GestureHandlerRootView } from "react-native-gesture-handler";

import PropCard from "@/components/PropCard";
import { useTheme } from "@/store/useTheme";
import { useSavedBets } from "@/store/useSavedBets";
import { useHistoricalPlayerTrends } from "@/hooks/useHistoricalPlayerTrends";
import { resolveSparklineByMarket } from "@/utils/resolveSparkline";

import { usePlayerPropsMaster } from "@/hooks/usePlayerPropsMaster";

export default function PropsTestScreen() {
  const colors = useTheme((s) => s.colors);
  const styles = useMemo(() => makeStyles(colors), [colors]);

  const savedIds = useSavedBets((s) => s.savedIds);
  const toggleSave = useSavedBets((s) => s.toggleSave);

  const {
    props,
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

  const renderItem = useCallback(
    ({ item }: any) => {
      const trend = getByPlayer(item.player);
      const spark = resolveSparklineByMarket(item.market, trend);

      return (
        <PropCard
          {...item}
          scrollRef={listRef}
          saved={savedIds.has(item.id)}
          onToggleSave={() => toggleSave(item.id)}
          expanded={expandedId === item.id}
          onToggleExpand={() => toggleExpand(item.id)}
          sparkline_l5={spark.sparkline_l5}
          sparkline_l10={spark.sparkline_l10}
          sparkline_l20={spark.sparkline_l20}
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

  return (
    <GestureHandlerRootView style={styles.root}>
      <View style={styles.screen}>
        {/* FILTERS */}
        <View style={styles.filters}>
          <Text style={styles.filtersTitle}>Filters</Text>

          {/* MARKET */}
          <View style={styles.pills}>
            {filters.markets.map((mkt) => {
              const active = filters.market === mkt;
              return (
                <Text
                  key={mkt}
                  onPress={() =>
                    setFilters((f) => ({
                      ...f,
                      market: active ? "ALL" : mkt,
                    }))
                  }
                  style={[
                    styles.pill,
                    active && styles.pillActive,
                  ]}
                >
                  {mkt}
                </Text>
              );
            })}
          </View>

          {/* HIT RATE WINDOW */}
          <View style={styles.pills}>
            {["L5", "L10", "L20"].map((w) => {
              const active = filters.hitRateWindow === w;
              return (
                <Text
                  key={w}
                  onPress={() =>
                    setFilters((f) => ({
                      ...f,
                      hitRateWindow: w as any,
                    }))
                  }
                  style={[
                    styles.pill,
                    active && styles.pillActive,
                  ]}
                >
                  {w}
                </Text>
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
            Confidence ≥ {filters.minConfidence}
          </Text>
          <Slider
            minimumValue={0}
            maximumValue={100}
            step={5}
            value={filters.minConfidence}
            onValueChange={(v) =>
              setFilters((f) => ({ ...f, minConfidence: v }))
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

        <FlatList
          ref={listRef}
          data={props}
          keyExtractor={(i) => i.id}
          renderItem={renderItem}
          showsVerticalScrollIndicator={false}
        />
      </View>
    </GestureHandlerRootView>
  );
}
