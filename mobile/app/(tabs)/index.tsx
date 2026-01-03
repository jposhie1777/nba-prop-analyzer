import { View, ScrollView, Text } from "react-native";
import { useMemo, useState, useEffect } from "react";
import Slider from "@react-native-community/slider";
import AsyncStorage from "@react-native-async-storage/async-storage";
import { GestureHandlerRootView } from "react-native-gesture-handler";

import PropCard from "../../components/PropCard";
import colors from "../../theme/color";
import { MOCK_PROPS } from "../../data/props";

// ---------------------------
// STORAGE KEYS
// ---------------------------
const FILTERS_KEY = "home_filters_v1";
const SAVED_PROPS_KEY = "saved_props_v1";

export default function HomeScreen() {
  // ---------------------------
  // TAB STATE (NEW)
  // ---------------------------
  const [activeTab, setActiveTab] = useState<"all" | "saved">("all");

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
  // SAVED PROPS STATE
  // ---------------------------
  const [savedIds, setSavedIds] = useState<Set<string>>(new Set());

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
  // LOAD SAVED PROPS
  // ---------------------------
  useEffect(() => {
    (async () => {
      try {
        const raw = await AsyncStorage.getItem(SAVED_PROPS_KEY);
        if (!raw) return;

        const ids: string[] = JSON.parse(raw);
        setSavedIds(new Set(ids));
      } catch (e) {
        console.warn("Failed to load saved props", e);
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
  // SAVE SAVED PROPS
  // ---------------------------
  useEffect(() => {
    AsyncStorage.setItem(
      SAVED_PROPS_KEY,
      JSON.stringify(Array.from(savedIds))
    );
  }, [savedIds]);

  // ---------------------------
  // TOGGLE SAVE
  // ---------------------------
  const toggleSave = (id: string) => {
    setSavedIds((prev) => {
      const next = new Set(prev);
      next.has(id) ? next.delete(id) : next.add(id);
      return next;
    });
  };

  // ---------------------------
  // DERIVE MARKETS
  // ---------------------------
  const markets = useMemo(
    () => Array.from(new Set(MOCK_PROPS.map((p) => p.market))),
    []
  );

  // ---------------------------
  // FILTER + SORT DATA
  // ---------------------------
  const filteredProps = useMemo(() => {
    return MOCK_PROPS
      .filter((p) => {
        // TAB LOGIC (NEW)
        if (activeTab === "saved" && !savedIds.has(p.id)) return false;

        if (marketFilter && p.market !== marketFilter) return false;
        if (evOnly && p.edge < 0.1) return false;
        if (p.confidence !== undefined && p.confidence < minConfidence)
          return false;
        if (p.odds < minOdds || p.odds > maxOdds) return false;

        return true;
      })
      .sort((a, b) => {
        if (sortBy === "edge") return b.edge - a.edge;
        return (b.confidence ?? 0) - (a.confidence ?? 0);
      });
  }, [
    activeTab,
    marketFilter,
    evOnly,
    minConfidence,
    minOdds,
    maxOdds,
    sortBy,
    savedIds,
  ]);

  // ---------------------------
  // RENDER
  // ---------------------------
  return (
    <GestureHandlerRootView style={{ flex: 1 }}>
      <View style={{ flex: 1, backgroundColor: colors.bg }}>
        {/* =========================
            TOP TABS (NEW)
        ========================== */}
        <View
          style={{
            flexDirection: "row",
            justifyContent: "space-around",
            paddingVertical: 10,
            borderBottomWidth: 1,
            borderBottomColor: colors.divider,
          }}
        >
          <Text
            onPress={() => setActiveTab("all")}
            style={{
              color:
                activeTab === "all"
                  ? colors.accent
                  : colors.textSecondary,
              fontWeight: "700",
            }}
          >
            All
          </Text>

          <Text
            onPress={() => setActiveTab("saved")}
            style={{
              color:
                activeTab === "saved"
                  ? colors.accent
                  : colors.textSecondary,
              fontWeight: "700",
            }}
          >
            Saved
          </Text>
        </View>

        {/* =========================
            FILTERS (UNCHANGED)
        ========================== */}
        <View style={{ padding: 12 }}>
          <Text
            onPress={() => setFiltersOpen(!filtersOpen)}
            style={{
              color: colors.textPrimary,
              fontWeight: "700",
              fontSize: 16,
            }}
          >
            Filters {filtersOpen ? "▲" : "▼"}
          </Text>

          {filtersOpen && (
            <>
              {/* MARKET PILLS */}
              <View style={{ flexDirection: "row", flexWrap: "wrap", marginTop: 8 }}>
                {markets.map((mkt) => {
                  const active = marketFilter === mkt;
                  return (
                    <Text
                      key={mkt}
                      onPress={() => setMarketFilter(active ? null : mkt)}
                      style={{
                        color: active ? colors.bg : colors.textSecondary,
                        backgroundColor: active
                          ? colors.accent
                          : "rgba(255,255,255,0.08)",
                        paddingHorizontal: 12,
                        paddingVertical: 6,
                        borderRadius: 14,
                        marginRight: 8,
                        marginBottom: 8,
                        fontWeight: "600",
                      }}
                    >
                      {mkt}
                    </Text>
                  );
                })}
              </View>

              {/* CONFIDENCE SLIDER */}
              <View style={{ marginTop: 12 }}>
                <Text style={{ color: colors.textSecondary }}>
                  Confidence ≥ {minConfidence}
                </Text>
                <Slider
                  minimumValue={0}
                  maximumValue={100}
                  step={5}
                  value={minConfidence}
                  onValueChange={setMinConfidence}
                  minimumTrackTintColor={colors.accent}
                  maximumTrackTintColor="rgba(255,255,255,0.2)"
                  thumbTintColor={colors.accent}
                />
              </View>
            </>
          )}
        </View>

        {/* =========================
            PROP CARDS (SWIPE ENABLED)
        ========================== */}
        <ScrollView showsVerticalScrollIndicator={false}>
          {filteredProps.map((prop) => (
            <PropCard
              key={prop.id}
              {...prop}
              saved={savedIds.has(prop.id)}
              onToggleSave={() => toggleSave(prop.id)}
            />
          ))}
        </ScrollView>
      </View>
    </GestureHandlerRootView>
  );
}