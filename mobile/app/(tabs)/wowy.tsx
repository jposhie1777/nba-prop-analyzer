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
import { Ionicons } from "@expo/vector-icons";

import { useTheme } from "@/store/useTheme";
import { useWowy } from "@/hooks/useWowy";
import { WowyCard } from "@/components/wowy/WowyCard";
import { TeamWowySection } from "@/components/wowy/TeamWowySection";
import { InjuredPlayerWowy, WowyStat } from "@/lib/wowy";

type StatusFilter = "all" | "out" | "questionable";
type ViewMode = "byTeam" | "impact";

function getImpactValue(player: InjuredPlayerWowy) {
  const diff = player.team_impact.team_ppg_diff;
  if (diff === null || diff === undefined) return Number.NEGATIVE_INFINITY;
  return Math.abs(diff);
}

export default function WowyScreen() {
  const { colors } = useTheme();
  const { injuredPlayers, season, loading, error, refresh } = useWowy();

  const [viewMode, setViewMode] = useState<ViewMode>("impact");
  const [statusFilter, setStatusFilter] = useState<StatusFilter>("out");
  const [searchQuery, setSearchQuery] = useState("");
  const [stat, setStat] = useState<WowyStat>("pts");

  /* =========================
     FILTERS
  ========================= */
  const filteredByStatus = injuredPlayers.filter((ip) => {
    if (statusFilter === "all") return true;
    if (statusFilter === "out") return ip.injured_player.status === "Out";
    if (statusFilter === "questionable") {
      return (
        ip.injured_player.status === "Questionable" ||
        ip.injured_player.status === "Doubtful"
      );
    }
    return true;
  });

  const filteredPlayers = searchQuery
    ? filteredByStatus.filter(
        (ip) =>
          ip.injured_player.player_name
            .toLowerCase()
            .includes(searchQuery.toLowerCase()) ||
          (ip.injured_player.team ?? "")
            .toLowerCase()
            .includes(searchQuery.toLowerCase())
      )
    : filteredByStatus;

  const impactSortedPlayers = [...filteredPlayers].sort((a, b) => {
    const aImpact = getImpactValue(a);
    const bImpact = getImpactValue(b);
    if (bImpact !== aImpact) return bImpact - aImpact;
    return a.injured_player.player_name.localeCompare(b.injured_player.player_name);
  });

  const groupedByTeam = filteredPlayers.reduce<Record<string, InjuredPlayerWowy[]>>(
    (acc, player) => {
      const teamKey = player.injured_player.team?.trim() || "Unknown";
      if (!acc[teamKey]) {
        acc[teamKey] = [];
      }
      acc[teamKey].push(player);
      return acc;
    },
    {}
  );

  const teamSections = Object.entries(groupedByTeam)
    .map(([team, players]) => ({
      team,
      players: [...players].sort((a, b) => {
        const aImpact = getImpactValue(a);
        const bImpact = getImpactValue(b);
        if (bImpact !== aImpact) return bImpact - aImpact;
        return a.injured_player.player_name.localeCompare(b.injured_player.player_name);
      }),
    }))
    .sort((a, b) => a.team.localeCompare(b.team));

  const teamCount = teamSections.length;
  const playerCount = filteredPlayers.length;

  /* =========================
     RENDER
  ========================= */
  return (
    <View
      style={[
        styles.container,
        { backgroundColor: colors.surface?.screen ?? "#111" },
      ]}
    >
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
            {season}-{String(season + 1).slice(2)} Season
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
          <Ionicons name="search" size={18} color={colors.text?.muted ?? "#888"} />
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

      {/* STAT TOGGLES */}
      <View style={styles.statToggleRow}>
        {[
          { key: "pts", label: "PTS" },
          { key: "reb", label: "REB" },
          { key: "ast", label: "AST" },
          { key: "fg3m", label: "3PM" },
        ].map((s) => (
          <Pressable
            key={s.key}
            onPress={() => setStat(s.key as WowyStat)}
            style={[
              styles.statToggleButton,
              {
                backgroundColor:
                  stat === s.key
                    ? colors.accent?.primary ?? "#3b82f6"
                    : colors.surface?.cardSoft ?? "#222",
              },
            ]}
          >
            <Text
              style={{
                color: stat === s.key ? "#fff" : colors.text?.muted ?? "#888",
                fontWeight: "700",
                fontSize: 12,
              }}
            >
              {s.label}
            </Text>
          </Pressable>
        ))}
      </View>

      {/* STATUS FILTER */}
      <View style={styles.filterRow}>
        {[
          { key: "out", label: "Out Only", color: "#ef4444" },
          { key: "questionable", label: "GTD", color: "#eab308" },
          { key: "all", label: "All", color: colors.accent?.primary ?? "#3b82f6" },
        ].map((f) => (
          <Pressable
            key={f.key}
            onPress={() => setStatusFilter(f.key as StatusFilter)}
            style={[
              styles.filterButton,
              {
                backgroundColor:
                  statusFilter === f.key
                    ? f.color
                    : colors.surface?.cardSoft ?? "#222",
              },
            ]}
          >
            <Text
              style={{
                color: statusFilter === f.key ? "#fff" : colors.text?.muted ?? "#888",
                fontSize: 13,
                fontWeight: "600",
              }}
            >
              {f.label}
            </Text>
          </Pressable>
        ))}
      </View>

      {/* VIEW TOGGLE */}
      <View style={styles.viewToggleRow}>
        <Pressable
          onPress={() => setViewMode("byTeam")}
          style={[
            styles.viewToggleButton,
            {
              backgroundColor:
                viewMode === "byTeam"
                  ? colors.accent?.primary ?? "#3b82f6"
                  : colors.surface?.cardSoft ?? "#222",
            },
          ]}
        >
          <Text
            style={[
              styles.viewToggleText,
              {
                color:
                  viewMode === "byTeam" ? "#fff" : colors.text?.muted ?? "#888",
              },
            ]}
          >
            By Team
          </Text>
        </Pressable>
        <Pressable
          onPress={() => setViewMode("impact")}
          style={[
            styles.viewToggleButton,
            {
              backgroundColor:
                viewMode === "impact"
                  ? colors.accent?.primary ?? "#3b82f6"
                  : colors.surface?.cardSoft ?? "#222",
            },
          ]}
        >
          <Text
            style={[
              styles.viewToggleText,
              {
                color:
                  viewMode === "impact" ? "#fff" : colors.text?.muted ?? "#888",
              },
            ]}
          >
            Largest Impact
          </Text>
        </Pressable>
      </View>

      <Text style={[styles.countText, { color: colors.text?.muted ?? "#888" }]}>
        {viewMode === "byTeam"
          ? `${teamCount} team${teamCount !== 1 ? "s" : ""} with ${playerCount} injured player${
              playerCount !== 1 ? "s" : ""
            }`
          : `${impactSortedPlayers.length} injured player${
              impactSortedPlayers.length !== 1 ? "s" : ""
            } with WOWY data`}
      </Text>

      {error && (
        <Text style={[styles.errorText, { color: colors.accent?.danger ?? "#ef4444" }]}>
          Error: {error}
        </Text>
      )}

      {viewMode === "byTeam" ? (
        <FlatList
          key="byTeam"
          contentContainerStyle={styles.listContent}
          data={teamSections}
          keyExtractor={(item) => item.team}
          renderItem={({ item }) => (
            <TeamWowySection
              team={item.team}
              players={item.players}
              stat={stat}
              defaultExpanded={false}
            />
          )}
          refreshControl={
            <RefreshControl refreshing={loading} onRefresh={refresh} />
          }
          ListEmptyComponent={
            !loading ? (
              <Text style={[styles.emptyText, { color: colors.text?.muted ?? "#888" }]}>
                No WOWY data found
              </Text>
            ) : null
          }
        />
      ) : (
        <FlatList
          key="impact"
          contentContainerStyle={styles.listContent}
          data={impactSortedPlayers}
          keyExtractor={(item) => String(item.injured_player.player_id)}
          renderItem={({ item }) => (
            <WowyCard data={item} stat={stat} defaultExpanded={false} />
          )}
          refreshControl={
            <RefreshControl refreshing={loading} onRefresh={refresh} />
          }
          ListEmptyComponent={
            !loading ? (
              <Text style={[styles.emptyText, { color: colors.text?.muted ?? "#888" }]}>
                No WOWY data found
              </Text>
            ) : null
          }
        />
      )}
    </View>
  );
}

/* =========================
   STYLES
========================= */
const styles = StyleSheet.create({
  container: { flex: 1 },
  header: { padding: 16, paddingBottom: 12 },
  title: { fontSize: 22, fontWeight: "700" },
  subtitle: { fontSize: 13, marginTop: 4 },
  seasonText: { fontSize: 11, marginTop: 4 },

  searchRow: { paddingHorizontal: 12, paddingVertical: 8 },
  searchContainer: {
    flexDirection: "row",
    alignItems: "center",
    paddingHorizontal: 12,
    paddingVertical: 10,
    borderRadius: 10,
    borderWidth: StyleSheet.hairlineWidth,
    gap: 8,
  },
  searchInput: { flex: 1, fontSize: 14 },

  statToggleRow: {
    flexDirection: "row",
    paddingHorizontal: 12,
    paddingBottom: 8,
    gap: 8,
  },
  statToggleButton: {
    flex: 1,
    paddingVertical: 8,
    borderRadius: 8,
    alignItems: "center",
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

  viewToggleRow: {
    flexDirection: "row",
    paddingHorizontal: 12,
    paddingBottom: 8,
    gap: 8,
  },
  viewToggleButton: {
    flex: 1,
    paddingVertical: 8,
    borderRadius: 8,
    alignItems: "center",
  },
  viewToggleText: {
    fontSize: 13,
    fontWeight: "600",
  },

  countText: { paddingHorizontal: 12, paddingBottom: 8, fontSize: 12 },
  listContent: { padding: 12, paddingBottom: 40 },
  errorText: { padding: 12, textAlign: "center" },
  emptyText: { textAlign: "center", marginTop: 20, fontSize: 13 },
});
