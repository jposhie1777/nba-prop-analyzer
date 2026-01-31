// app/(tabs)/wowy.tsx
import React, { useState } from "react";
import {
  View,
  Text,
  FlatList,
  RefreshControl,
  StyleSheet,
  Pressable,
  TextInput,
} from "react-native";
import { useTheme } from "@/store/useTheme";
import { useWowy } from "@/hooks/useWowy";
import { WowyCard } from "@/components/wowy/WowyCard";
import { Ionicons } from "@expo/vector-icons";

type StatusFilter = "all" | "out" | "questionable";

export default function WowyScreen() {
  const { colors } = useTheme();
  const { injuredPlayers, count, season, loading, error, refresh } = useWowy();

  const [statusFilter, setStatusFilter] = useState<StatusFilter>("out");
  const [searchQuery, setSearchQuery] = useState("");

  // Filter by status
  const filteredByStatus = injuredPlayers.filter((ip) => {
    if (statusFilter === "all") return true;
    if (statusFilter === "out") return ip.injured_player.status === "Out";
    if (statusFilter === "questionable")
      return (
        ip.injured_player.status === "Questionable" ||
        ip.injured_player.status === "Doubtful"
      );
    return true;
  });

  // Filter by search query
  const filteredPlayers = searchQuery
    ? filteredByStatus.filter(
        (ip) =>
          ip.injured_player.player_name
            .toLowerCase()
            .includes(searchQuery.toLowerCase()) ||
          ip.injured_player.team
            .toLowerCase()
            .includes(searchQuery.toLowerCase())
      )
    : filteredByStatus;

  // Sort by team PPG impact (most negative impact first = most important players)
  const sortedPlayers = [...filteredPlayers].sort((a, b) => {
    const aImpact = a.team_impact.team_ppg_diff ?? 0;
    const bImpact = b.team_impact.team_ppg_diff ?? 0;
    return aImpact - bImpact; // Most negative (biggest impact) first
  });

  return (
    <View style={[styles.container, { backgroundColor: colors.surface?.screen ?? "#111" }]}>
      {/* Header */}
      <View
        style={[
          styles.header,
          { backgroundColor: colors.surface?.cardSoft ?? "#1a1a1a" },
        ]}
      >
        <Text style={[styles.title, { color: colors.text?.primary ?? "#fff" }]}>
          With Or Without You
        </Text>
        <Text style={[styles.subtitle, { color: colors.text?.muted ?? "#888" }]}>
          How teammates perform when a player is OUT
        </Text>
        {season && (
          <Text style={[styles.seasonText, { color: colors.text?.muted ?? "#888" }]}>
            {season - 1}-{String(season).slice(2)} Season
          </Text>
        )}
      </View>

      {/* Search */}
      <View style={styles.searchRow}>
        <View
          style={[
            styles.searchContainer,
            {
              backgroundColor: colors.surface?.card ?? "#1a1a1a",
              borderColor: colors.border?.subtle ?? "#333",
            },
          ]}
        >
          <Ionicons
            name="search"
            size={18}
            color={colors.text?.muted ?? "#888"}
          />
          <TextInput
            style={[styles.searchInput, { color: colors.text?.primary ?? "#fff" }]}
            placeholder="Search player or team..."
            placeholderTextColor={colors.text?.muted ?? "#888"}
            value={searchQuery}
            onChangeText={setSearchQuery}
          />
          {searchQuery.length > 0 && (
            <Pressable onPress={() => setSearchQuery("")}>
              <Ionicons
                name="close-circle"
                size={18}
                color={colors.text?.muted ?? "#888"}
              />
            </Pressable>
          )}
        </View>
      </View>

      {/* Status Filter */}
      <View style={styles.filterRow}>
        <Pressable
          onPress={() => setStatusFilter("out")}
          style={[
            styles.filterButton,
            {
              backgroundColor:
                statusFilter === "out"
                  ? "#ef4444"
                  : colors.surface?.cardSoft ?? "#222",
            },
          ]}
        >
          <Text
            style={[
              styles.filterText,
              {
                color: statusFilter === "out" ? "#fff" : colors.text?.muted ?? "#888",
              },
            ]}
          >
            Out Only
          </Text>
        </Pressable>

        <Pressable
          onPress={() => setStatusFilter("questionable")}
          style={[
            styles.filterButton,
            {
              backgroundColor:
                statusFilter === "questionable"
                  ? "#eab308"
                  : colors.surface?.cardSoft ?? "#222",
            },
          ]}
        >
          <Text
            style={[
              styles.filterText,
              {
                color:
                  statusFilter === "questionable"
                    ? "#fff"
                    : colors.text?.muted ?? "#888",
              },
            ]}
          >
            GTD
          </Text>
        </Pressable>

        <Pressable
          onPress={() => setStatusFilter("all")}
          style={[
            styles.filterButton,
            {
              backgroundColor:
                statusFilter === "all"
                  ? colors.accent?.primary ?? "#3b82f6"
                  : colors.surface?.cardSoft ?? "#222",
            },
          ]}
        >
          <Text
            style={[
              styles.filterText,
              {
                color: statusFilter === "all" ? "#fff" : colors.text?.muted ?? "#888",
              },
            ]}
          >
            All
          </Text>
        </Pressable>
      </View>

      {/* Count */}
      <Text style={[styles.countText, { color: colors.text?.muted ?? "#888" }]}>
        {sortedPlayers.length} injured player{sortedPlayers.length !== 1 ? "s" : ""} with WOWY data
      </Text>

      {/* Error State */}
      {error && (
        <Text style={[styles.errorText, { color: colors.accent?.danger ?? "#ef4444" }]}>
          Error: {error}
        </Text>
      )}

      {/* Content */}
      <FlatList
        contentContainerStyle={styles.listContent}
        data={sortedPlayers}
        keyExtractor={(item) => String(item.injured_player.player_id)}
        renderItem={({ item }) => (
          <WowyCard data={item} defaultExpanded={sortedPlayers.length <= 3} />
        )}
        refreshControl={
          <RefreshControl refreshing={loading} onRefresh={refresh} />
        }
        ListEmptyComponent={
          !loading ? (
            <View style={styles.emptyContainer}>
              <Ionicons
                name="analytics-outline"
                size={48}
                color={colors.text?.muted ?? "#888"}
              />
              <Text style={[styles.emptyText, { color: colors.text?.muted ?? "#888" }]}>
                No WOWY data available
              </Text>
              <Text
                style={[styles.emptySubtext, { color: colors.text?.muted ?? "#888" }]}
              >
                {searchQuery
                  ? "Try a different search"
                  : "Make sure injury data has been ingested"}
              </Text>
            </View>
          ) : null
        }
      />
    </View>
  );
}

const styles = StyleSheet.create({
  container: {
    flex: 1,
  },
  header: {
    padding: 16,
    paddingBottom: 12,
  },
  title: {
    fontSize: 22,
    fontWeight: "700",
  },
  subtitle: {
    fontSize: 13,
    marginTop: 4,
  },
  seasonText: {
    fontSize: 11,
    marginTop: 4,
  },
  searchRow: {
    paddingHorizontal: 12,
    paddingVertical: 8,
  },
  searchContainer: {
    flexDirection: "row",
    alignItems: "center",
    paddingHorizontal: 12,
    paddingVertical: 10,
    borderRadius: 10,
    borderWidth: StyleSheet.hairlineWidth,
    gap: 8,
  },
  searchInput: {
    flex: 1,
    fontSize: 14,
  },
  filterRow: {
    flexDirection: "row",
    paddingHorizontal: 12,
    paddingBottom: 8,
    gap: 8,
  },
  filterButton: {
    flex: 1,
    paddingVertical: 8,
    borderRadius: 8,
    alignItems: "center",
  },
  filterText: {
    fontSize: 13,
    fontWeight: "600",
  },
  countText: {
    paddingHorizontal: 12,
    paddingBottom: 8,
    fontSize: 12,
  },
  listContent: {
    padding: 12,
    paddingBottom: 40,
  },
  errorText: {
    padding: 12,
    textAlign: "center",
  },
  emptyContainer: {
    alignItems: "center",
    marginTop: 60,
  },
  emptyText: {
    fontSize: 16,
    marginTop: 16,
  },
  emptySubtext: {
    fontSize: 13,
    marginTop: 8,
    textAlign: "center",
  },
});
