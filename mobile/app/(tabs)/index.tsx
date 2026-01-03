import { View, ScrollView, Text } from "react-native";
import { useMemo, useState } from "react";
import PropCard from "../../components/PropCard";
import colors from "../../theme/color";
import { MOCK_PROPS } from "../../data/props";

export default function HomeScreen() {
  const [marketFilter, setMarketFilter] = useState<string | null>(null);
  const [minConfidence, setMinConfidence] = useState(0);
  const [evOnly, setEvOnly] = useState(false);
  const [sortBy, setSortBy] = useState<"edge" | "confidence">("edge");

  const filteredProps = useMemo(() => {
    return MOCK_PROPS
      .filter((p) => {
        if (marketFilter && p.market !== marketFilter) return false;
        if (p.confidence !== undefined && p.confidence < minConfidence) return false;
        if (evOnly && p.edge < 0.1) return false;
        return true;
      })
      .sort((a, b) => {
        if (sortBy === "edge") return b.edge - a.edge;
        return (b.confidence ?? 0) - (a.confidence ?? 0);
      });
  }, [marketFilter, minConfidence, evOnly, sortBy]);

  return (
    <View style={{ flex: 1, backgroundColor: colors.bg }}>
      {/* Filter / Sort Controls */}
      <View style={{ padding: 12 }}>
        <Text style={{ color: colors.textPrimary, fontWeight: "600" }}>
          Filters
        </Text>

        <Text
          style={{ color: colors.accent, marginTop: 4 }}
          onPress={() =>
            setSortBy(sortBy === "edge" ? "confidence" : "edge")
          }
        >
          Sort: {sortBy === "edge" ? "Edge ↓" : "Confidence ↓"} (tap)
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
      </View>

      {/* Cards */}
      <ScrollView showsVerticalScrollIndicator={false}>
        {filteredProps.map((prop) => (
          <PropCard key={prop.id} {...prop} />
        ))}
      </ScrollView>
    </View>
  );
}