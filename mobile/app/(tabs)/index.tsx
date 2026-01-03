import { View, ScrollView, Text } from "react-native";
import { useMemo, useState } from "react";
import Slider from "@react-native-community/slider";
import PropCard from "../../components/PropCard";
import colors from "../../theme/color";
import { MOCK_PROPS } from "../../data/props";

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

  // ---------------------------
  // DERIVE AVAILABLE MARKETS
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
        if (marketFilter && p.market !== marketFilter) return false;
        if (evOnly && p.edge < 0.1) return false;
        if (p.confidence !== undefined && p.confidence < minConfidence) return false;
        if (p.odds < minOdds || p.odds > maxOdds) return false;
        return true;
      })
      .sort((a, b) => {
        if (sortBy === "edge") return b.edge - a.edge;
        return (b.confidence ?? 0) - (a.confidence ?? 0);
      });
  }, [marketFilter, evOnly, minConfidence, minOdds, maxOdds, sortBy]);

  // ---------------------------
  // RENDER
  // ---------------------------
  return (
    <View style={{ flex: 1, backgroundColor: colors.bg }}>
      {/* =========================
          FILTER / SORT HEADER
      ========================== */}
      <View style={{ padding: 12 }}>
        {/* Title */}
        <Text
          style={{
            color: colors.textPrimary,
            fontWeight: "700",
            marginBottom: 6,
          }}
        >
          Filters
        </Text>

        {/* MARKET PILLS */}
        <View style={{ flexDirection: "row", flexWrap: "wrap" }}>
          {markets.map((mkt) => {
            const active = marketFilter === mkt;

            return (
              <Text
                key={mkt}
                onPress={() =>
                  setMarketFilter(active ? null : mkt)
                }
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

        {/* ODDS FILTER */}
        <View style={{ marginTop: 16 }}>
          <Text style={{ color: colors.textSecondary, marginBottom: 6 }}>
            Odds Range
          </Text>
        
          <Text style={{ color: colors.textSecondary, fontSize: 12 }}>
            Min: {minOdds}
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
        
          <Text style={{ color: colors.textSecondary, fontSize: 12, marginTop: 8 }}>
            Max: {maxOdds}
          </Text>
        
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
        {/* SORT TOGGLE */}
        <Text
          style={{ color: colors.accent, marginTop: 4 }}
          onPress={() =>
            setSortBy(sortBy === "edge" ? "confidence" : "edge")
          }
        >
          Sort: {sortBy === "edge" ? "Edge ↓" : "Confidence ↓"} (tap)
        </Text>

        {/* EV TOGGLE */}
        <Text
          style={{
            color: evOnly ? colors.success : colors.textSecondary,
            marginTop: 8,
          }}
          onPress={() => setEvOnly(!evOnly)}
        >
          {evOnly ? "✓ +EV Only" : "+EV Only"}
        </Text>
      </View>

      {/* =========================
          PROP CARDS
      ========================== */}
      <ScrollView showsVerticalScrollIndicator={false}>
        {filteredProps.map((prop) => (
          <PropCard key={prop.id} {...prop} />
        ))}
      </ScrollView>
    </View>
  );
}