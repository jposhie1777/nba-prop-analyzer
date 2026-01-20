// app/(tabs)/index
import {
  View,
  Text,
  StyleSheet,
  FlatList,
  Pressable, // ✅ ADD THIS
} from "react-native";
import { useMemo, useState, useEffect, useCallback, useRef } from "react";
import Slider from "@react-native-community/slider";
import AsyncStorage from "@react-native-async-storage/async-storage";
import { GestureHandlerRootView } from "react-native-gesture-handler";

import PropCard from "../../components/PropCard";
import { useTheme } from "@/store/useTheme";
import { fetchProps, MobileProp } from "../../lib/api";
import { useSavedBets } from "@/store/useSavedBets";
import { usePropsStore } from "@/store/usePropsStore";
import { themeMeta } from "@/theme/meta";
import { useHistoricalPlayerTrends } from "@/hooks/useHistoricalPlayerTrends";
import { resolveSparklineByMarket } from "@/utils/resolveSparkline";


// ---------------------------
// STORAGE KEYS
// ---------------------------
const FILTERS_KEY = "home_filters_v1";
const SAVED_PROPS_KEY = "saved_props_v1";

// ---------------------------
// UI-SAFE PROP MODEL
// ---------------------------
type UIProp = {
  id: string;
  edge: number;
  confidence: number;

  home?: string;
  away?: string;
} & MobileProp;



// ---------------------------
// MULTI-BOOK GROUPING
// ---------------------------
type GroupedProp = UIProp & {
  books?: {
    bookmaker: string;
    odds: number;
  }[];
};

export default function HomeScreen() {
  // ---------------------------
  // TAB STATE
  // ---------------------------
  const [activeTab, setActiveTab] = useState<"all" | "saved">("all");

  // ---------------------------
  // SAVED BETS (GLOBAL STORE)
  // ---------------------------
  const savedIds = useSavedBets((s) => s.savedIds);
  const toggleSave = useSavedBets((s) => s.toggleSave);
  
  const setPropsStore = usePropsStore((s) => s.setProps);

  // ---------------------------
  // REAL PROPS DATA
  // ---------------------------
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const { ready: trendsReady, getByPlayer } = useHistoricalPlayerTrends();
  const PAGE_SIZE = 200;

  const [props, setProps] = useState<UIProp[]>([]);
  const [offset, setOffset] = useState(0);
  const [hasMore, setHasMore] = useState(true);
  const [loadingMore, setLoadingMore] = useState(false);

  
  const listRef = useRef<FlatList>(null);

  // ---------------------------
  // FILTER + SORT STATE
  // ---------------------------
  const [marketFilter, setMarketFilter] = useState<string | null>(null);
  const [evOnly, setEvOnly] = useState(false);
  const [sortBy, setSortBy] = useState<"edge" | "confidence">("edge");

  const [minConfidence, setMinConfidence] = useState(0);
  const [minOdds, setMinOdds] = useState(-300);
  const [maxOdds, setMaxOdds] = useState(300);

  const [filtersOpen, setFiltersOpen] = useState(true);
  const colors = useTheme((s) => s.colors);
  const setTheme = useTheme((s) => s.setTheme);
  
  const styles = useMemo(() => makeStyles(colors), [colors]);
  
  const [themeOpen, setThemeOpen] = useState(false);
  // ---------------------------
  // EXPANDED CARD STATE (ONLY ONE OPEN)
  // ---------------------------
  const [expandedId, setExpandedId] = useState<string | null>(null);
  
  const toggleExpand = useCallback((id: string) => {
    setExpandedId((prev) => (prev === id ? null : id));
  }, []);

  // ---------------------------
  // LOAD SAVED FILTERS
  // ---------------------------
  useEffect(() => {
    (async () => {
      try {
        const raw = await AsyncStorage.getItem(FILTERS_KEY);
        if (!raw) return;

        const saved = JSON.parse(raw);
        setMarketFilter(saved.marketFilter ?? null);
        setEvOnly(saved.evOnly ?? false);
        setSortBy(saved.sortBy ?? "edge");
        setMinConfidence(saved.minConfidence ?? 0);
        setMinOdds(saved.minOdds ?? -300);
        setMaxOdds(saved.maxOdds ?? 300);
        setFiltersOpen(saved.filtersOpen ?? true);
      } catch (e) {
        console.warn("Failed to load filters", e);
      }
    })();
  }, []);

  // ---------------------------
  // SAVE FILTERS
  // ---------------------------
  useEffect(() => {
    AsyncStorage.setItem(
      FILTERS_KEY,
      JSON.stringify({
        marketFilter,
        evOnly,
        sortBy,
        minConfidence,
        minOdds,
        maxOdds,
        filtersOpen,
      })
    );
  }, [
    marketFilter,
    evOnly,
    sortBy,
    minConfidence,
    minOdds,
    maxOdds,
    filtersOpen,
  ]);

  // ---------------------------
  // LOAD PROPS FROM API
  // ---------------------------
  const loadMoreProps = useCallback(async () => {
    if (loadingMore || !hasMore) return;
  
    setLoadingMore(true);
    setError(null);
  
    try {
      const res = await fetchProps({
        minHitRate: 0,
        limit: PAGE_SIZE,
        offset,
      });
  
      const normalized: UIProp[] = res.props.map((p) => ({
        ...p,
        id: `${p.player}-${p.market}-${p.line}`,
        edge: p.hitRateL10 ?? 0,
        confidence: p.confidence_score,
        matchup: p.matchup,
        bookmaker: p.bookmaker,
        home: p.home_team,
        away: p.away_team,
      }));
  
      setProps((prev) => {
        const next = [...prev, ...normalized];
        setPropsStore(next);
        return next;
      });
  
      setOffset((prev) => prev + PAGE_SIZE);
  
      if (res.props.length < PAGE_SIZE) {
        setHasMore(false); // ✅ no more pages
      }
    } catch (err) {
      setError(String(err));
    } finally {
      setLoading(false);        // initial screen only
      setLoadingMore(false);   // pagination spinner
    }
  }, [offset, loadingMore, hasMore, setPropsStore]);

  const didInit = useRef(false);

  /* ---------------------------
     INITIAL LOAD (STRICT-MODE SAFE)
  --------------------------- */
  useEffect(() => {
    if (didInit.current) return;
    didInit.current = true;
  
    setLoading(true);
    loadMoreProps();
  }, [loadMoreProps]);

  // ---------------------------
  // DERIVE MARKETS
  // ---------------------------
  const markets = useMemo(() => {
    return Array.from(new Set(props.map((p) => p.market)));
  }, [props]);

  // ---------------------------
  // FILTER + SORT + GROUP
  // ---------------------------
  const filteredProps = useMemo(() => {
    const base = props
      .filter((p) => {
        if (activeTab === "saved" && !savedIds.has(p.id)) return false;
        if (marketFilter && p.market !== marketFilter) return false;
        if (evOnly && p.edge < 0.1) return false;
        if (p.confidence < minConfidence) return false;
        if (p.odds < minOdds || p.odds > maxOdds) return false;
        return true;
      })
      .sort((a, b) => {
        if (sortBy === "edge") return b.edge - a.edge;
        return b.confidence - a.confidence;
      });

    const map = new Map<string, GroupedProp>();
    base.forEach((p) => {
      const key = `${p.player}-${p.market}-${p.line}`;
      if (!map.has(key)) map.set(key, { ...p, books: [] });
      if (p.bookmaker) {
        map.get(key)!.books!.push({
          bookmaker: p.bookmaker,
          odds: p.odds,
        });
      }
    });

    return Array.from(map.values());
  }, [
    props,
    activeTab,
    savedIds,
    marketFilter,
    evOnly,
    minConfidence,
    minOdds,
    maxOdds,
    sortBy,
  ]);

  // helper: normalize null → undefined for PropCard props
  const n = (v: number | null | undefined) => v ?? undefined;

  // ---------------------------
  // FLATLIST RENDER ITEM
  // ---------------------------
  const renderItem = useCallback(
    ({ item }: { item: GroupedProp }) => {
      const trend = getByPlayer(item.player);
      const spark = resolveSparklineByMarket(item.market, trend);
      const last5_dates = trend?.last5_dates;
      const last10_dates = trend?.last10_dates;
      const last20_dates = trend?.last20_dates;
      return (
        <PropCard
          /* CORE */
          player={item.player}
          market={item.market}
          line={item.line}
          odds={item.odds}
          books={item.books}
          playerImageUrl={item.player_image_url}
          side={item.side}
          scrollRef={listRef}
  
          hitRateL10={item.hitRateL10}
          edge={item.edge}
          confidence={item.confidence}
  
          /* WINDOW METRICS */
          avg_l5={n(item.avg_l5)}
          avg_l10={n(item.avg_l10)}
          avg_l20={n(item.avg_l20)}
  
          hit_rate_l5={n(item.hit_rate_l5)}
          hit_rate_l10={n(item.hit_rate_l10)}
          hit_rate_l20={n(item.hit_rate_l20)}
  
          clear_1p_pct_l5={n(item.clear_1p_pct_l5)}
          clear_1p_pct_l10={n(item.clear_1p_pct_l10)}
          clear_1p_pct_l20={n(item.clear_1p_pct_l20)}
  
          clear_2p_pct_l5={n(item.clear_2p_pct_l5)}
          clear_2p_pct_l10={n(item.clear_2p_pct_l10)}
          clear_2p_pct_l20={n(item.clear_2p_pct_l20)}
  
          avg_margin_l5={n(item.avg_margin_l5)}
          avg_margin_l10={n(item.avg_margin_l10)}
          avg_margin_l20={n(item.avg_margin_l20)}
  
          bad_miss_pct_l5={n(item.bad_miss_pct_l5)}
          bad_miss_pct_l10={n(item.bad_miss_pct_l10)}
          bad_miss_pct_l20={n(item.bad_miss_pct_l20)}
  
          pace_l5={n(item.pace_l5)}
          pace_l10={n(item.pace_l10)}
          pace_l20={n(item.pace_l20)}
  
          usage_l5={n(item.usage_l5)}
          usage_l10={n(item.usage_l10)}
          usage_l20={n(item.usage_l20)}
  
          /* CONTEXT */
          ts_l10={n(item.ts_l10)}
          pace_delta={n(item.pace_delta)}
          delta_vs_line={n(item.delta_vs_line)}
  
          sparkline_l5={spark.sparkline_l5}
          sparkline_l10={spark.sparkline_l10}
          sparkline_l20={spark.sparkline_l20}
        
          last5_dates={last5_dates}
          last10_dates={last10_dates}
          last20_dates={last20_dates}
        
          matchup={item.matchup}
          home={item.home}
          away={item.away}
  
          /* STATE */
          saved={savedIds.has(item.id)}
          onToggleSave={() => toggleSave(item.id)}
  
          expanded={expandedId === item.id}
          onToggleExpand={() => toggleExpand(item.id)}
        />
      );
    },
    [savedIds, toggleSave, expandedId, toggleExpand, getByPlayer]
  );


  // ---------------------------
  // STATES
  // ---------------------------
  if (error) {
    return (
      <GestureHandlerRootView style={styles.root}>
        <View style={styles.center}>
          <Text style={styles.error}>{error}</Text>
        </View>
      </GestureHandlerRootView>
    );
  }

  if (loading) {
    return (
      <GestureHandlerRootView style={styles.root}>
        <View style={styles.center}>
          <Text style={styles.loading}>Loading today’s props…</Text>
        </View>
      </GestureHandlerRootView>
    );
  }

  // ---------------------------
  // RENDER
  // ---------------------------
  return (
    <GestureHandlerRootView style={styles.root}>
      <View style={styles.screen}>
        {/* =========================
            TOP TABS
        ========================== */}
        <View style={styles.tabs}>
          <Text
            onPress={() => setActiveTab("all")}
            style={[styles.tab, activeTab === "all" && styles.tabActive]}
          >
            All
          </Text>
          <Text
            onPress={() => setActiveTab("saved")}
            style={[styles.tab, activeTab === "saved" && styles.tabActive]}
          >
            Saved
          </Text>
        </View>

        {/* =========================
            FILTERS
        ========================== */}
        <View style={styles.filters}>
          {/* HEADER ROW */}
          <View style={styles.filterHeaderRow}>
            <Text
              onPress={() => setFiltersOpen((v) => !v)}
              style={styles.filtersTitle}
            >
              Filters {filtersOpen ? "▲" : "▼"}
            </Text>
        
            <Pressable
              onPress={() => setThemeOpen(true)}
              style={styles.themeBtn}
            >
              <Text style={styles.themeBtnText}>🎨 Theme</Text>
            </Pressable>
          </View>

          {filtersOpen && (
            <View style={styles.filtersCard}>
              {/* MARKET PILLS */}
              <View style={styles.pills}>
                {markets.map((mkt) => {
                  const active = marketFilter === mkt;
                  return (
                    <Text
                      key={mkt}
                      onPress={() =>
                        setMarketFilter(active ? null : mkt)
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

              {/* CONFIDENCE */}
              <Text style={styles.sliderLabel}>
                Confidence ≥ {minConfidence}
              </Text>
              <Slider
                minimumValue={0}
                maximumValue={100}
                step={5}
                value={minConfidence}
                onValueChange={setMinConfidence}
                minimumTrackTintColor={colors.accent.primary}
                maximumTrackTintColor={colors.surface.cardSoft}
                thumbTintColor={colors.accent.primary}

              />

              {/* ODDS */}
              <Text style={styles.sliderLabel}>Min Odds: {minOdds}</Text>
              <Slider
                minimumValue={-1000}
                maximumValue={1000}
                step={25}
                value={minOdds}
                onValueChange={setMinOdds}
                minimumTrackTintColor={colors.accent.primary}
                maximumTrackTintColor={colors.surface.cardSoft}
                thumbTintColor={colors.accent.primary}
              />


              <Text style={styles.sliderLabel}>Max Odds: {maxOdds}</Text>
              <Slider
                minimumValue={-1000}
                maximumValue={1000}
                step={25}
                value={maxOdds}
                onValueChange={setMaxOdds}
                minimumTrackTintColor={colors.accent.primary}
                maximumTrackTintColor={colors.surface.cardSoft}
                thumbTintColor={colors.accent.primary}
              />

            </View>
          )}
        </View>

        {/* =========================
            PROP LIST
        ========================== */}
        <FlatList
          ref={listRef}
          data={filteredProps}
          keyExtractor={(item) => item.id}
          renderItem={renderItem}
          showsVerticalScrollIndicator={false}
          contentContainerStyle={styles.list}
        
          /* 🔽 PAGINATION */
          onEndReached={() => {
            if (!loadingMore && hasMore) {
              loadMoreProps();
            }
          }}
          onEndReachedThreshold={0.6}
        
          /* 🔽 FOOTER */
          ListFooterComponent={
            loadingMore ? (
              <View style={{ paddingVertical: 24 }}>
                <Text style={{ textAlign: "center", color: colors.text.muted }}>
                  Loading more props…
                </Text>
              </View>
            ) : (
              <View style={{ height: 40 }} />
            )
          }
        
          /* 🔽 PERF (IMPORTANT) */
          removeClippedSubviews={false}
          initialNumToRender={10}
          maxToRenderPerBatch={12}
          windowSize={7}
/>
        {themeOpen && (
          <View style={styles.themeOverlay}>
            <View style={styles.themeModal}>
              {Object.entries(themeMeta).map(([key, meta]) => (
                <Pressable
                  key={key}
                  onPress={() => {
                    setTheme(key as any);
                    setThemeOpen(false);
                  }}
                  style={[
                    styles.themeOption,
                    { backgroundColor: meta.preview },
                  ]}
                >
                  <Text style={styles.themeLabel}>{meta.label}</Text>
                </Pressable>
              ))}
            </View>
          </View>
        )}
      </View>
    </GestureHandlerRootView>
  );
}

// ---------------------------
// STYLES — HOME SCREEN
// ---------------------------

const makeStyles = (colors: any) =>
  StyleSheet.create({
    root: { flex: 1 },

    screen: {
      flex: 1,
      backgroundColor: colors.surface.screen,
    },

    center: {
      flex: 1,
      justifyContent: "center",
      alignItems: "center",
    },

    error: {
      color: colors.accent.danger,
      fontWeight: "600",
    },

    loading: {
      color: colors.text.muted,
    },

    tabs: {
      flexDirection: "row",
      justifyContent: "space-around",
      paddingVertical: 12,
      backgroundColor: colors.surface.card,
      borderBottomWidth: 1,
      borderBottomColor: colors.border.subtle,
    },

    tab: {
      fontWeight: "700",
      color: colors.text.muted,
    },

    tabActive: {
      color: colors.accent.primary,
    },

    filters: {
      padding: 14,
      backgroundColor: colors.surface.card,
      borderBottomWidth: 1,
      borderBottomColor: colors.border.subtle,
    },

    filterHeaderRow: {
      flexDirection: "row",
      justifyContent: "space-between",
      alignItems: "center",
    },

    filtersTitle: {
      fontSize: 16,
      fontWeight: "700",
      color: colors.text.primary,
    },

    filtersCard: {
      backgroundColor: colors.surface.cardSoft,
      borderRadius: 14,
      padding: 12,
      marginTop: 10,
      borderWidth: 1,
      borderColor: colors.border.subtle,
    },

    pills: {
      flexDirection: "row",
      flexWrap: "wrap",
      marginBottom: 12,
    },

    pill: {
      paddingHorizontal: 12,
      paddingVertical: 6,
      borderRadius: 14,
      marginRight: 8,
      marginBottom: 8,
      fontWeight: "600",
      color: colors.text.secondary,
      backgroundColor: colors.surface.elevated,
    },

    pillActive: {
      backgroundColor: colors.accent.primary,
      color: colors.text.primary,
    },

    sliderLabel: {
      marginTop: 10,
      marginBottom: 4,
      color: colors.text.secondary,
      fontWeight: "600",
    },

    list: {
      paddingTop: 8,
    },

    themeBtn: {
      paddingHorizontal: 10,
      paddingVertical: 6,
      borderRadius: 10,
      backgroundColor: colors.surface.elevated,
      borderWidth: 1,
      borderColor: colors.border.subtle,
    },

    themeBtnText: {
      fontSize: 12,
      fontWeight: "800",
      color: colors.text.primary,
    },

    themeOverlay: {
      position: "absolute",
      inset: 0,
      backgroundColor: "rgba(0,0,0,0.4)",
      justifyContent: "center",
      alignItems: "center",
    },

    themeModal: {
      backgroundColor: colors.surface.card,
      padding: 16,
      borderRadius: 16,
      width: "80%",
      gap: 10,
    },

    themeOption: {
      padding: 12,
      borderRadius: 12,
    },

    themeLabel: {
      fontWeight: "800",
      color: colors.text.primary,
    },
  });
