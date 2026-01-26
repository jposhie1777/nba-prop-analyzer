import { View, Text, Pressable, StyleSheet } from "react-native";
import Slider from "@react-native-community/slider";
import { useTheme } from "@/store/useTheme";

/* ======================================================
   CONSTANTS
====================================================== */

const STATS = ["pts", "reb", "ast", "3pm"];
const MARKET_TYPES = ["OVER", "UNDER", "MILESTONE"];

/* ======================================================
   COMPONENT
====================================================== */

export function LivePropFilterBar({
  filters,
  setFilters,
}: {
  filters: {
    stats: string[];
    marketTypes: string[];
    minOdds: number;
    maxOdds: number;
  };
  setFilters: (f: any) => void;
}) {
  const { colors } = useTheme();

  /* ---------------------------------
     TOGGLE HELPERS
  ---------------------------------- */
  const toggle = (key: "stats" | "marketTypes", value: string) => {
    setFilters((prev: any) => {
      const set = new Set(prev[key]);
      set.has(value) ? set.delete(value) : set.add(value);
      return { ...prev, [key]: Array.from(set) };
    });
  };

  /* ---------------------------------
     RESET
  ---------------------------------- */
  const resetFilters = () => {
    setFilters({
      stats: [],
      marketTypes: [],
      minOdds: -800,
      maxOdds: 400,
    });
  };

  /* ======================================================
     RENDER
  ===================================================== */

  return (
    <View
      style={[
        styles.container,
        { backgroundColor: colors.surface.card },
      ]}
    >
      {/* HEADER */}
      <View style={styles.headerRow}>
        <Text style={[styles.title, { color: colors.text.primary }]}>
          Filters
        </Text>

        <Pressable onPress={resetFilters}>
          <Text style={[styles.reset, { color: colors.text.muted }]}>
            Reset
          </Text>
        </Pressable>
      </View>

      {/* =========================
          STAT FILTER
      ========================== */}
      <Text style={[styles.label, { color: colors.text.secondary }]}>
        Stats
      </Text>
      <View style={styles.row}>
        {STATS.map((s) => {
          const active = filters.stats.includes(s);
          return (
            <Pressable
              key={s}
              onPress={() => toggle("stats", s)}
            >
              <Text
                style={[
                  styles.pill,
                  active && {
                    backgroundColor: colors.accent.primary,
                    color: "#fff",
                  },
                ]}
              >
                {s.toUpperCase()}
              </Text>
            </Pressable>
          );
        })}
      </View>

      {/* =========================
          MARKET TYPE FILTER
      ========================== */}
      <Text style={[styles.label, { color: colors.text.secondary }]}>
        Market
      </Text>
      <View style={styles.row}>
        {MARKET_TYPES.map((m) => {
          const active = filters.marketTypes.includes(m);
          return (
            <Pressable
              key={m}
              onPress={() => toggle("marketTypes", m)}
            >
              <Text
                style={[
                  styles.pill,
                  active && {
                    backgroundColor: colors.accent.primary,
                    color: "#fff",
                  },
                ]}
              >
                {m}
              </Text>
            </Pressable>
          );
        })}
      </View>

      {/* =========================
          ODDS RANGE
      ========================== */}
      <Text style={[styles.label, { color: colors.text.secondary }]}>
        Odds: {filters.minOdds} to {filters.maxOdds}
      </Text>

      {/* MIN */}
      <Slider
        minimumValue={-1000}
        maximumValue={1000}
        step={10}
        value={filters.minOdds}
        onValueChange={(v) =>
          setFilters((f: any) => ({ ...f, minOdds: v }))
        }
        minimumTrackTintColor={colors.accent.primary}
      />

      {/* MAX */}
      <Slider
        minimumValue={-1000}
        maximumValue={1000}
        step={10}
        value={filters.maxOdds}
        onValueChange={(v) =>
          setFilters((f: any) => ({ ...f, maxOdds: v }))
        }
        minimumTrackTintColor={colors.accent.primary}
      />
    </View>
  );
}

/* ======================================================
   STYLES
====================================================== */

const styles = StyleSheet.create({
  container: {
    padding: 12,
    borderBottomWidth: 1,
    borderColor: "#E5E7EB",
  },

  headerRow: {
    flexDirection: "row",
    justifyContent: "space-between",
    alignItems: "center",
    marginBottom: 8,
  },

  title: {
    fontSize: 14,
    fontWeight: "900",
  },

  reset: {
    fontSize: 12,
    fontWeight: "700",
  },

  label: {
    fontSize: 12,
    fontWeight: "800",
    marginTop: 10,
    marginBottom: 6,
  },

  row: {
    flexDirection: "row",
    flexWrap: "wrap",
    gap: 8,
  },

  pill: {
    paddingHorizontal: 12,
    paddingVertical: 6,
    borderRadius: 999,
    backgroundColor: "#E5E7EB",
    fontSize: 11,
    fontWeight: "800",
    color: "#111",
  },
});