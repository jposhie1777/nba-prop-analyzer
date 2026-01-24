// app/(tabs)/props-test.tsx
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

/* ======================================================
   Screen
====================================================== */
export default function PropsTestScreen() {
  const { colors } = useTheme();
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
     SANITIZE + NORMALIZE DATA
  ====================================================== */
  const props = useMemo(() => {
    return rawProps
      .filter((p) => {
        // hard guardrails
        if (!p.player) return false;
        if (!p.prop_type_base) return false;
        if (p.line_value == null) return false;
        return true;
      })
      .map((p, idx) => {
        const market = String(p.prop_type_base || "unknown");

        return {
          ...p,
          market,
          line: p.line_value,

          // 🔑 bulletproof ID (no collisions)
          id: [
            p.player_id ?? "p",
            market,
            p.line_value,
            p.market_window ?? "FULL",
            idx,
          ].join("::"),
        };
      });
  }, [rawProps]);

  /* ======================================================
     RENDER ITEM
  ====================================================== */
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

          {/* MARKETS */}
          <View style={styles.pills}>
            {filters.markets.map((mkt, i) => {
              const active = filters.market === mkt;
              return (
                <Pressable
                  key={`mkt-${mkt}-${i}`}
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

        {/* ================= LIST ================= */}
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