import { View, ScrollView, Text } from "react-native";
import { useMemo, useState, useEffect } from "react";
import Slider from "@react-native-community/slider";
import AsyncStorage from "@react-native-async-storage/async-storage";

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
  const [savedOnly, setSavedOnly] = useState(false);

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
        setSavedOnly(saved.savedOnly ?? false);
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
        savedOnly,
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
    savedOnly,
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
        if (savedOnly && !savedIds.has(p.id)) return false;
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
    marketFilter,
    evOnly,
    minConfidence,
    minOdds,
    maxOdds,
    sortBy,
    savedOnly,
    savedIds,
  ]);

  // ---------------------------
  // RENDER
  // ---------------------------
  return (
    <View style={{ flex: 1, backgroundColor: colors.bg }}>
      {/* =========================
          COLLAPSIBLE FILTER HEADER
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

            {/* ODDS SLIDERS */}
            <View style={{ marginTop: 16 }}>
              <Text style={{ color: colors.textSecondary }}>
                Odds {minOdds} → {maxOdds}
              </Text>

              <Slider
                minimumValue={-300}
                maximumValue={300}
                step={10}
                value={minOdds}
                onValueChange={setMinOdds}
                minimumTrackTintColor={colors.accent}
                maximumTrackTintColor="rgba(255,255,255,0.2)"
                thumbTintColor={colors.accent}
              />

              <Slider
                minimumValue={-300}
                maximumValue={300}
                step={10}
                value={maxOdds}
                onValueChange={setMaxOdds}
                minimumTrackTintColor={colors.accent}
                maximumTrackTintColor="rgba(255,255,255,0.2)"
                thumbTintColor={colors.accent}
              />
            </View>

            {/* SORT / EV / SAVED */}
            <Text
              style={{ color: colors.accent, marginTop: 12 }}
              onPress={() =>
                setSortBy(sortBy === "edge" ? "confidence" : "edge")
              }
            >
              Sort: {sortBy === "edge" ? "Edge ↓" : "Confidence ↓"}
            </Text>

            <Text
              style={{
                color: evOnly ? colors.success : colors.textSecondary,
                marginTop: 8,
              }}
              onPress={() => setEvOnly(!evOnly)}
            >
              {evOnly ? "✓ +EV Only" : "+EV Only"}
            </Text>

            <Text
              style={{
                color: savedOnly ? colors.accent : colors.textSecondary,
                marginTop: 8,
              }}
              onPress={() => setSavedOnly(!savedOnly)}
            >
              {savedOnly ? "★ Saved Only" : "☆ Saved Only"}
            </Text>
          </>
        )}
      </View>

      {/* =========================
          PROP CARDS
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
  );
}