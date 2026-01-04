import {
  View,
  Text,
  StyleSheet,
  FlatList,
} from "react-native";
import { useMemo, useState, useEffect, useCallback } from "react";
import Slider from "@react-native-community/slider";
import AsyncStorage from "@react-native-async-storage/async-storage";
import { GestureHandlerRootView } from "react-native-gesture-handler";

import PropCard from "../../components/PropCard";
import colors from "../../theme/color";
import { fetchProps, MobileProp } from "../../lib/api";
import { useSavedBets } from "@/store/useSavedBets";
import { usePropsStore } from "@/store/usePropsStore";

// ---------------------------
// STORAGE KEYS
// ---------------------------
const FILTERS_KEY = "home_filters_v1";
const SAVED_PROPS_KEY = "saved_props_v1";

// ---------------------------
// UI-SAFE PROP MODEL
// ---------------------------
type UIProp = MobileProp & {
  id: string;
  edge: number;
  confidence: number;
};

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
  const [props, setProps] = useState<UIProp[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);

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
  useEffect(() => {
    let mounted = true;
    setLoading(true);
    setError(null);

    fetchProps({
      gameDate: "2026-01-03",
      minHitRate: 0,
      limit: 200,
    })
      .then((res) => {
        if (!mounted) return;

        const normalized: UIProp[] = res.props.map((p) => {
          const hitRate = p.hitRateL10 ?? 0;
          return {
            ...p,
            id: `${p.player}-${p.market}-${p.line}`,
            edge: hitRate,
            confidence: Math.round(hitRate * 100),
            matchup: p.matchup,
            bookmaker: p.bookmaker,
            home: p.home_team,
            away: p.away_team,
          };
        });

        setProps(normalized);
        setPropsStore(normalized);

        setLoading(false);
      })
      .catch((err) => {
        if (!mounted) return;
        setError(String(err));
        setLoading(false);
      });

    return () => {
      mounted = false;
    };
  }, []);

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

  // ---------------------------
  // FLATLIST RENDER ITEM
  // ---------------------------
  const renderItem = useCallback(
    ({ item }: { item: GroupedProp }) => (
      <PropCard
        {...item}
        books={item.books}
        saved={savedIds.has(item.id)}
        onToggleSave={() => toggleSave(item.id)}
  
        /* EXPAND CONTROL */
        expanded={expandedId === item.id}
        onToggleExpand={() => toggleExpand(item.id)}
      />
    ),
    [savedIds, toggleSave, expandedId, toggleExpand]
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
          <Text
            onPress={() => setFiltersOpen((v) => !v)}
            style={styles.filtersTitle}
          >
            Filters {filtersOpen ? "▲" : "▼"}
          </Text>

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
                minimumTrackTintColor={colors.accent}
                maximumTrackTintColor="#E5E7EB"
                thumbTintColor={colors.accent}
              />

              {/* ODDS */}
              <Text style={styles.sliderLabel}>Min Odds: {minOdds}</Text>
              <Slider
                minimumValue={-1000}
                maximumValue={1000}
                step={25}
                value={minOdds}
                onValueChange={setMinOdds}
                minimumTrackTintColor={colors.accent}
                maximumTrackTintColor="#E5E7EB"
                thumbTintColor={colors.accent}
              />

              <Text style={styles.sliderLabel}>Max Odds: {maxOdds}</Text>
              <Slider
                minimumValue={-1000}
                maximumValue={1000}
                step={25}
                value={maxOdds}
                onValueChange={setMaxOdds}
                minimumTrackTintColor={colors.accent}
                maximumTrackTintColor="#E5E7EB"
                thumbTintColor={colors.accent}
              />
            </View>
          )}
        </View>

        {/* =========================
            PROP LIST
        ========================== */}
        <FlatList
          data={filteredProps}
          keyExtractor={(item) => item.id}
          renderItem={renderItem}
          showsVerticalScrollIndicator={false}
          contentContainerStyle={styles.list}
          ListFooterComponent={<View style={{ height: 40 }} />}
        />
      </View>
    </GestureHandlerRootView>
  );
}

// ---------------------------
// STYLES — LIGHT MODE
// ---------------------------
const styles = StyleSheet.create({
  root: { flex: 1 },

  screen: {
    flex: 1,
    backgroundColor: "#F5F7FB",
  },

  center: {
    flex: 1,
    justifyContent: "center",
    alignItems: "center",
  },

  error: {
    color: "#DC2626",
    fontWeight: "600",
  },

  loading: {
    color: "#6B7280",
  },

  tabs: {
    flexDirection: "row",
    justifyContent: "space-around",
    paddingVertical: 12,
    backgroundColor: "#FFFFFF",
    borderBottomWidth: 1,
    borderBottomColor: "#E5E7EB",
  },

  tab: {
    fontWeight: "700",
    color: "#6B7280",
  },

  tabActive: {
    color: colors.accent,
  },

  filters: {
    padding: 14,
    backgroundColor: "#F9FAFB",
    borderBottomWidth: 1,
    borderBottomColor: "#E5E7EB",
  },

  filtersTitle: {
    fontSize: 16,
    fontWeight: "700",
    color: "#111827",
  },

  filtersCard: {
    backgroundColor: "#FFFFFF",
    borderRadius: 14,
    padding: 12,
    marginTop: 10,
    borderWidth: 1,
    borderColor: "#E5E7EB",
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
    color: "#374151",
    backgroundColor: "#E5E7EB",
  },

  pillActive: {
    backgroundColor: colors.accent,
    color: "#FFFFFF",
  },

  sliderLabel: {
    marginTop: 10,
    marginBottom: 4,
    color: "#374151",
    fontWeight: "600",
  },

  list: {
    paddingTop: 8,
  },
});
