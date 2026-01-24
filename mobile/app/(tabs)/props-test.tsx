// app/(tabs)/props-test.tsx
import {
  View,
  Text,
  StyleSheet,
  FlatList,
  Pressable,
} from "react-native";
import { useMemo, useState, useCallback, useRef } from "react";
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

  /* ======================================================
     Render Item
  ====================================================== */
  const renderItem = useCallback(
    ({ item }: any) => {
      const trend = getByPlayer(item.player);
      const spark = resolveSparklineByMarket(item.market, trend);

      return (
        <PropCard
          /* CORE */
          player={item.player}
          market={item.market}
          line={item.line}
          odds={item.odds}
          side={item.odds === item.odds_under ? "under" : "over"}
          playerImageUrl={item.player_image_url}

          /* HIT RATES (DEFAULT = L10) */
          hitRateL10={item.hit_rate_over_l10}
          edge={0}
          confidence={0}

          /* WINDOWS */
          avg_l5={item.avg_l5}
          avg_l10={item.avg_l10}
          avg_l20={item.avg_l20}

          hit_rate_l5={item.hit_rate_over_l5}
          hit_rate_l10={item.hit_rate_over_l10}
          hit_rate_l20={item.hit_rate_over_l20}

          /* CONTEXT */
          matchup={item.matchup}
          home={item.home_team}
          away={item.away_team}

          /* SPARKLINES */
          sparkline_l5={spark.sparkline_l5}
          sparkline_l10={spark.sparkline_l10}
          sparkline_l20={spark.sparkline_l20}
          last5_dates={trend?.last5_dates}
          last10_dates={trend?.last10_dates}
          last20_dates={trend?.last20_dates}

          /* STATE */
          scrollRef={listRef}
          saved={savedIds.has(item.id)}
          onToggleSave={() => toggleSave(item.id)}
          expanded={expandedId === item.id}
          onToggleExpand={() => toggleExpand(item.id)}
        />
      );
    },
    [savedIds, expandedId, toggleSave, toggleExpand, getByPlayer]
  );

  /* ======================================================
     Loading State
  ====================================================== */
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
     Render
  ====================================================== */
  return (
    <GestureHandlerRootView style={styles.root}>
      <View style={styles.screen}>
        {/* ===========================
            FILTERS
        ============================ */}
        <View style={styles.filters}>
          <Text style={styles.filtersTitle}>Filters</Text>

          {/* MARKET */}
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

          {/* HIT RATE WINDOW */}
          <View style={styles.pills}>
            {(["L5", "L10", "L20"] as const).map((w) => {
              const active = filters.hitRateWindow === w;
              return (
                <Pressable
                  key={w}
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

          {/* HIT RATE SLIDER */}
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

          {/* ODDS RANGE */}
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
            minimumTrackTintColor={colors.accent.primary}
            thumbTintColor={colors.accent.primary}
          />

          <Slider
            minimumValue={-1000}
            maximumValue={1000}
            step={25}
            value={filters.maxOdds}
            onValueChange={(v) =>
              setFilters((f) => ({ ...f, maxOdds: v }))
            }
            minimumTrackTintColor={colors.accent.primary}
            thumbTintColor={colors.accent.primary}
          />
        </View>

        {/* ===========================
            PROP LIST
        ============================ */}
        <FlatList
          ref={listRef}
          data={props}
          keyExtractor={(item) => item.id}
          renderItem={renderItem}
          showsVerticalScrollIndicator={false}
          contentContainerStyle={{ paddingBottom: 40 }}
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
      fontWeight: "600",
    },

    filters: {
      padding: 14,
      backgroundColor: colors.surface.card,
      borderBottomWidth: 1,
      borderBottomColor: colors.border.subtle,
    },

    filtersTitle: {
      fontSize: 16,
      fontWeight: "800",
      color: colors.text.primary,
      marginBottom: 10,
    },

    pills: {
      flexDirection: "row",
      flexWrap: "wrap",
      gap: 8,
      marginBottom: 10,
    },

    pill: {
      paddingHorizontal: 12,
      paddingVertical: 6,
      borderRadius: 14,
      backgroundColor: colors.surface.elevated,
      color: colors.text.secondary,
      fontWeight: "700",
    },

    pillActive: {
      backgroundColor: colors.accent.primary,
      color: colors.text.primary,
    },

    sliderLabel: {
      marginTop: 10,
      marginBottom: 4,
      fontWeight: "700",
      color: colors.text.secondary,
    },
  });
}
