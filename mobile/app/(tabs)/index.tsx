import { View, ScrollView, Text } from "react-native";
import { useMemo, useState, useEffect } from "react";
import Slider from "@react-native-community/slider";
import AsyncStorage from "@react-native-async-storage/async-storage";
import { GestureHandlerRootView } from "react-native-gesture-handler";

import PropCard from "../../components/PropCard";
import colors from "../../theme/color";
import { fetchProps, MobileProp } from "../../lib/api";

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
  edge: number;        // 0–1 (proxy for now)
  confidence: number;  // 0–100
};

export default function HomeScreen() {
  // ---------------------------
  // TAB STATE
  // ---------------------------
  const [activeTab, setActiveTab] = useState<"all" | "saved">("all");

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
  // LOAD PROPS FROM API
  // ---------------------------
  useEffect(() => {
    let mounted = true;

    setLoading(true);
    setError(null);

    fetchProps({
      gameDate: "2026-01-03", // later: derive dynamically
      minHitRate: 0,
      limit: 200,
    })
      .then((res) => {
        if (!mounted) return;

        const normalized: UIProp[] = res.props.map((p) => {
          const hitRate = p.hitRateL10 ?? 0;

          return {
            ...p,

            // ---------- REQUIRED UI FIELDS ----------
            id: `${p.player}-${p.market}-${p.line}`,

            edge: hitRate,
            confidence: Math.round(hitRate * 100),

            // ---------- CARD DISPLAY FIELDS ----------
            matchup: p.matchup,                 // "MIN @ MIA"
            bookmaker: p.bookmaker,             // "fanduel" | "draftkings"
            home: p.home_team,                  // "MIA"
            away: p.away_team,                  // "MIN"
          };

        });

        setProps(normalized);
        setLoading(false);
      })
      .catch((err) => {
        console.error("FETCH ERROR:", err);
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
  // FILTER + SORT DATA
  // ---------------------------
  const filteredProps = useMemo(() => {
    return props
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
  // ERROR STATE
  // ---------------------------
  if (error) {
    return (
      <GestureHandlerRootView style={{ flex: 1 }}>
        <View
          style={{
            flex: 1,
            backgroundColor: colors.bg,
            justifyContent: "center",
          }}
        >
          <Text style={{ color: "red", textAlign: "center" }}>
            {error}
          </Text>
        </View>
      </GestureHandlerRootView>
    );
  }

  // ---------------------------
  // LOADING STATE
  // ---------------------------
  if (loading) {
    return (
      <GestureHandlerRootView style={{ flex: 1 }}>
        <View
          style={{
            flex: 1,
            backgroundColor: colors.bg,
            justifyContent: "center",
          }}
        >
          <Text style={{ color: colors.textSecondary, textAlign: "center" }}>
            Loading today’s props…
          </Text>
        </View>
      </GestureHandlerRootView>
    );
  }

  // ---------------------------
  // RENDER
  // ---------------------------
  return (
    <GestureHandlerRootView style={{ flex: 1 }}>
      <View style={{ flex: 1, backgroundColor: colors.bg }}>
        {/* =========================
            TOP TABS
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
            FILTERS
        ========================== */}
        <View style={{ padding: 12 }}>
          <Text
            onPress={() => setFiltersOpen((v) => !v)}
            style={{
              color: colors.textPrimary,
              fontWeight: "700",
              fontSize: 16,
            }}
          >
            Filters {filtersOpen ? "▲" : "▼"}
          </Text>

          {filtersOpen && (
            <View
              style={{
                backgroundColor: colors.card,
                borderRadius: 14,
                padding: 12,
                marginTop: 10,
              }}
            >
              {/* MARKET PILLS */}
              <View
                style={{
                  flexDirection: "row",
                  flexWrap: "wrap",
                  marginBottom: 12,
                }}
              >
                {markets.map((mkt) => {
                  const active = marketFilter === mkt;
                  return (
                    <Text
                      key={mkt}
                      onPress={() =>
                        setMarketFilter(active ? null : mkt)
                      }
                      style={{
                        color: active
                          ? colors.bg
                          : colors.textSecondary,
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
              <View style={{ marginBottom: 16 }}>
                <Text style={{ color: colors.textSecondary, marginBottom: 4 }}>
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
              <View>
                <Text style={{ color: colors.textSecondary, marginBottom: 4 }}>
                  Min Odds: {minOdds}
                </Text>
                <Slider
                  minimumValue={-1000}
                  maximumValue={1000}
                  step={25}
                  value={minOdds}
                  onValueChange={setMinOdds}
                  minimumTrackTintColor={colors.accent}
                  maximumTrackTintColor="rgba(255,255,255,0.2)"
                  thumbTintColor={colors.accent}
                />

                <Text
                  style={{
                    color: colors.textSecondary,
                    marginTop: 12,
                    marginBottom: 4,
                  }}
                >
                  Max Odds: {maxOdds}
                </Text>
                <Slider
                  minimumValue={-1000}
                  maximumValue={1000}
                  step={25}
                  value={maxOdds}
                  onValueChange={setMaxOdds}
                  minimumTrackTintColor={colors.accent}
                  maximumTrackTintColor="rgba(255,255,255,0.2)"
                  thumbTintColor={colors.accent}
                />
              </View>

              {/* RESET */}
              <View style={{ marginTop: 12 }}>
                <Text
                  onPress={() => {
                    setMarketFilter(null);
                    setEvOnly(false);
                    setSortBy("edge");
                    setMinConfidence(0);
                    setMinOdds(-300);
                    setMaxOdds(300);
                  }}
                  style={{
                    color: colors.accent,
                    fontWeight: "700",
                    textAlign: "right",
                  }}
                >
                  Reset
                </Text>
              </View>
            </View>
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
    </GestureHandlerRootView>
  );
}
