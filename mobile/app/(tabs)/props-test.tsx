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
import * as Haptics from "expo-haptics";

import PropCard from "@/components/PropCard";
import { useTheme } from "@/store/useTheme";
import { useSavedBets } from "@/store/useSavedBets";
import { useHistoricalPlayerTrends } from "@/hooks/useHistoricalPlayerTrends";
import { resolveSparklineByMarket } from "@/utils/resolveSparkline";
import { usePlayerPropsMaster } from "@/hooks/usePlayerPropsMaster";
import { useBetslipDrawer } from "@/store/useBetslipDrawer";
import { PropBetslipDrawer } from "@/components/prop/PropBetslipDrawer";

/* ======================================================
   Screen
====================================================== */
export default function PropsTestScreen() {
  const colors = useTheme((s) => s.colors);
  const styles = useMemo(() => makeStyles(colors), [colors]);

  const savedIds = useSavedBets((s) => s.savedIds);
  const toggleSave = useSavedBets((s) => s.toggleSave);
  const openBetslip = useBetslipDrawer((s) => s.open);
  const addToBetslip = usePropBetslip((s) => s.add);
  const removeFromBetslip = usePropBetslip((s) => s.remove);

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
     SAVE / UNSAVE (single source of truth)
  ====================================================== */
  const saveProp = useCallback(
    (item: any) => {
      if (savedIds.has(item.id)) return;
  
      toggleSave(item.id);
  
      addToBetslip({
        id: item.id,
        player: item.player,
        market: item.market,
        side: item.side ?? "over",
        line: item.line,
        odds: item.odds,
        matchup: item.matchup,
      });
  
      Haptics.impactAsync(Haptics.ImpactFeedbackStyle.Medium);
  
      // ✅ OPEN THE DRAWER
      openBetslip();
    },
    [savedIds, toggleSave, addToBetslip, openBetslip]
  );

  const unsaveProp = useCallback(
    (id: string) => {
      toggleSave(id);
      removeFromBetslip(id);
    },
    [toggleSave, removeFromBetslip]
  );

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

        if (
          filters.marketWindow &&
          p.market_window !== filters.marketWindow
        ) {
          return false;
        }

        const hitRate =
          filters.hitRateWindow === "L5"
            ? p.hit_rate_l5
            : filters.hitRateWindow === "L10"
            ? p.hit_rate_l10
            : p.hit_rate_l20;

        if (hitRate != null && hitRate < filters.minHitRate) {
          return false;
        }

        if (p.odds < filters.minOdds) return false;
        if (p.odds > filters.maxOdds) return false;

        return true;
      })
      .map((p, idx) => ({
        ...p,
        id: `${p.id}::${idx}`,
      }));

    cleaned.sort((a, b) => {
      const getHR = (p: any) =>
        filters.hitRateWindow === "L5"
          ? p.hit_rate_l5 ?? 0
          : filters.hitRateWindow === "L10"
          ? p.hit_rate_l10 ?? 0
          : p.hit_rate_l20 ?? 0;

      const hrDiff = getHR(b) - getHR(a);
      if (hrDiff !== 0) return hrDiff;

      return (a.odds ?? 0) - (b.odds ?? 0);
    });

    return cleaned;
  }, [rawProps, filters]);

  /* ======================================================
     RENDER ITEM
  ====================================================== */
  const renderItem = useCallback(
    ({ item }: any) => {
      const trend = getByPlayer(item.player);
      const spark = resolveSparklineByMarket(item.market, trend);
      const isSaved = savedIds.has(item.id);

      return (
        <PropCard
          {...item}
          playerId={item.player_id}
          scrollRef={listRef}
          saved={isSaved}
          onSwipeSave={() => saveProp(item)}
          onToggleSave={() =>
            isSaved ? unsaveProp(item.id) : saveProp(item)
          }
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
    [
      savedIds,
      expandedId,
      toggleExpand,
      getByPlayer,
      saveProp,
      unsaveProp,
    ]
  );

  if (loading) {
    return (
      <GestureHandlerRootView style={styles.root}>
        <View style={styles.screen}>
          <View style={styles.filters}>
            {/* filters */}
          </View>
    
          <FlatList
            ref={listRef}
            data={props}
            keyExtractor={(item) => item.id}
            renderItem={renderItem}
            showsVerticalScrollIndicator={false}
          />
        </View>
    
        {/* ✅ THIS IS THE RENDER YOU WERE ASKING ABOUT */}
        <PropBetslipDrawer />
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

          <View style={styles.pills}>
            {(["FULL", "Q1", "FIRST3MIN"] as const).map((w) => {
              const active = filters.marketWindow === w;
              return (
                <Pressable
                  key={w}
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
    screen: { flex: 1, backgroundColor: colors.surface.screen },
    center: { flex: 1, alignItems: "center", justifyContent: "center" },
    loading: { color: colors.text.muted },

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