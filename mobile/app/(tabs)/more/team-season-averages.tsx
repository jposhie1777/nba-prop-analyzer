// app/(tabs)/more/team-season-averages.tsx
import {
  View,
  Text,
  TextInput,
  FlatList,
  Pressable,
  ActivityIndicator,
  ScrollView,
} from "react-native";
import { SafeAreaView } from "react-native-safe-area-context";
import { useState, useMemo } from "react";
import { Stack } from "expo-router";

import { useTheme } from "@/store/useTheme";
import {
  useTeamSeasonAverages,
  TeamSeasonAveragesRow,
} from "@/hooks/useTeamSeasonAverages";

type SortKey = keyof TeamSeasonAveragesRow;
type SortDir = "asc" | "desc";

export default function TeamSeasonAveragesScreen() {
  const { colors } = useTheme();

  const [search, setSearch] = useState("");
  const [debouncedSearch, setDebouncedSearch] = useState("");
  const [sortKey, setSortKey] = useState<SortKey>("w");
  const [sortDir, setSortDir] = useState<SortDir>("desc");

  // Debounce search input
  const handleSearchChange = (text: string) => {
    setSearch(text);
    setTimeout(() => setDebouncedSearch(text), 300);
  };

  const { rows, count, loading, error, refetch } = useTeamSeasonAverages({
    search: debouncedSearch,
    limit: 30,
  });

  // Sort data locally
  const sortedRows = useMemo(() => {
    if (!rows.length) return [];

    return [...rows].sort((a, b) => {
      const aVal = a[sortKey];
      const bVal = b[sortKey];

      if (aVal == null && bVal == null) return 0;
      if (aVal == null) return 1;
      if (bVal == null) return -1;

      if (typeof aVal === "string" && typeof bVal === "string") {
        return sortDir === "asc"
          ? aVal.localeCompare(bVal)
          : bVal.localeCompare(aVal);
      }

      return sortDir === "asc"
        ? (aVal as number) - (bVal as number)
        : (bVal as number) - (aVal as number);
    });
  }, [rows, sortKey, sortDir]);

  const toggleSort = (key: SortKey) => {
    if (sortKey === key) {
      setSortDir((d) => (d === "asc" ? "desc" : "asc"));
    } else {
      setSortKey(key);
      setSortDir("desc");
    }
  };

  // Column config
  const columns: { key: SortKey; label: string; width: number }[] = [
    { key: "team_abbreviation", label: "Team", width: 60 },
    { key: "team_conference", label: "Conf", width: 55 },
    { key: "w", label: "W", width: 45 },
    { key: "l", label: "L", width: 45 },
    { key: "w_pct", label: "PCT", width: 55 },
    { key: "pts", label: "PTS", width: 55 },
    { key: "reb", label: "REB", width: 55 },
    { key: "ast", label: "AST", width: 55 },
    { key: "stl", label: "STL", width: 50 },
    { key: "blk", label: "BLK", width: 50 },
    { key: "tov", label: "TOV", width: 50 },
    { key: "fg_pct", label: "FG%", width: 60 },
    { key: "fg3_pct", label: "3P%", width: 60 },
    { key: "ft_pct", label: "FT%", width: 60 },
    { key: "plus_minus", label: "+/-", width: 55 },
  ];

  const formatValue = (key: SortKey, val: any) => {
    if (val == null) return "—";
    if (key === "w_pct") return val.toFixed(3);
    if (key.endsWith("_pct")) return (val * 100).toFixed(1);
    if (typeof val === "number" && !Number.isInteger(val)) {
      return val.toFixed(1);
    }
    return String(val);
  };

  return (
    <>
      <Stack.Screen
        options={{
          title: "Team Season Averages",
          headerStyle: { backgroundColor: colors.surface.screen },
          headerTintColor: colors.text.primary,
        }}
      />

      <SafeAreaView
        edges={["bottom"]}
        style={{ flex: 1, backgroundColor: colors.surface.screen }}
      >
        {/* Search Bar */}
        <View style={{ padding: 12, paddingBottom: 8 }}>
          <TextInput
            value={search}
            onChangeText={handleSearchChange}
            placeholder="Search teams..."
            placeholderTextColor={colors.text.muted}
            style={{
              backgroundColor: colors.surface.card,
              borderRadius: 12,
              paddingHorizontal: 16,
              paddingVertical: 12,
              fontSize: 16,
              color: colors.text.primary,
              borderWidth: 1,
              borderColor: colors.border.subtle,
            }}
          />
          <Text
            style={{
              color: colors.text.muted,
              fontSize: 12,
              marginTop: 6,
              marginLeft: 4,
            }}
          >
            {count} teams • Tap column headers to sort
          </Text>
        </View>

        {/* Loading State */}
        {loading && (
          <View style={{ padding: 20, alignItems: "center" }}>
            <ActivityIndicator color={colors.accent.primary} />
          </View>
        )}

        {/* Error State */}
        {error && (
          <View style={{ padding: 20 }}>
            <Text style={{ color: colors.text.danger }}>{error}</Text>
            <Pressable onPress={refetch} style={{ marginTop: 12 }}>
              <Text style={{ color: colors.accent.primary }}>Retry</Text>
            </Pressable>
          </View>
        )}

        {/* Table */}
        {!loading && !error && (
          <View
            style={{
              flex: 1,
              margin: 12,
              marginTop: 0,
              backgroundColor: colors.surface.card,
              borderRadius: 12,
              borderWidth: 1,
              borderColor: colors.border.subtle,
              overflow: "hidden",
            }}
          >
            <ScrollView horizontal showsHorizontalScrollIndicator={false}>
              <View>
                {/* Header */}
                <View
                  style={{
                    flexDirection: "row",
                    paddingVertical: 10,
                    paddingHorizontal: 8,
                    backgroundColor: colors.surface.cardSoft,
                    borderBottomWidth: 1,
                    borderColor: colors.border.subtle,
                  }}
                >
                  {columns.map((col) => {
                    const isActive = sortKey === col.key;
                    return (
                      <Pressable
                        key={col.key}
                        onPress={() => toggleSort(col.key)}
                        style={{ width: col.width }}
                      >
                        <Text
                          style={{
                            fontSize: 11,
                            fontWeight: "700",
                            color: isActive
                              ? colors.accent.primary
                              : colors.text.secondary,
                          }}
                          numberOfLines={1}
                        >
                          {col.label}
                          {isActive ? (sortDir === "asc" ? " ▲" : " ▼") : ""}
                        </Text>
                      </Pressable>
                    );
                  })}
                </View>

                {/* Rows */}
                <FlatList
                  data={sortedRows}
                  keyExtractor={(item) => String(item.team_id)}
                  showsVerticalScrollIndicator={false}
                  contentContainerStyle={{ paddingBottom: 12 }}
                  renderItem={({ item, index }) => {
                    const isEven = index % 2 === 0;
                    return (
                      <View
                        style={{
                          flexDirection: "row",
                          paddingVertical: 8,
                          paddingHorizontal: 8,
                          backgroundColor: isEven
                            ? colors.surface.card
                            : colors.surface.cardSoft,
                          borderBottomWidth: 1,
                          borderColor: colors.border.subtle,
                        }}
                      >
                        {columns.map((col) => (
                          <Text
                            key={col.key}
                            style={{
                              width: col.width,
                              fontSize: 12,
                              color: colors.text.primary,
                              fontWeight:
                                col.key === "team_abbreviation"
                                  ? "700"
                                  : "400",
                            }}
                            numberOfLines={1}
                          >
                            {formatValue(col.key, item[col.key])}
                          </Text>
                        ))}
                      </View>
                    );
                  }}
                />
              </View>
            </ScrollView>
          </View>
        )}
      </SafeAreaView>
    </>
  );
}
