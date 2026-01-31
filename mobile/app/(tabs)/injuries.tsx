// app/(tabs)/injuries.tsx
import React, { useState } from "react";
import {
  View,
  Text,
  FlatList,
  RefreshControl,
  StyleSheet,
  Pressable,
  ScrollView,
} from "react-native";
import { useTheme } from "@/store/useTheme";
import { useInjuries } from "@/hooks/useInjuries";
import { InjuryCard } from "@/components/injuries/InjuryCard";
import { TeamInjuriesSection } from "@/components/injuries/TeamInjuriesSection";

type ViewMode = "byTeam" | "all";
type StatusFilter = "all" | "out" | "questionable" | "probable";

export default function InjuriesScreen() {
  const { colors } = useTheme();
  const { injuries, byTeam, statusSummary, loading, error, refresh } =
    useInjuries();

  const [viewMode, setViewMode] = useState<ViewMode>("byTeam");
  const [statusFilter, setStatusFilter] = useState<StatusFilter>("all");

  // Filter injuries based on status
  const filteredInjuries = injuries.filter((inj) => {
    if (statusFilter === "all") return true;
    if (statusFilter === "out") return inj.status === "Out";
    if (statusFilter === "questionable")
      return inj.status === "Questionable" || inj.status === "Doubtful";
    if (statusFilter === "probable")
      return inj.status === "Probable" || inj.status === "Day-To-Day";
    return true;
  });

  // Filter by team as well
  const filteredByTeam = byTeam
    .map((team) => ({
      ...team,
      injuries: team.injuries.filter((inj) => {
        if (statusFilter === "all") return true;
        if (statusFilter === "out") return inj.status === "Out";
        if (statusFilter === "questionable")
          return inj.status === "Questionable" || inj.status === "Doubtful";
        if (statusFilter === "probable")
          return inj.status === "Probable" || inj.status === "Day-To-Day";
        return true;
      }),
    }))
    .filter((team) => team.injuries.length > 0);

  return (
    <View style={[styles.container, { backgroundColor: colors.surface?.screen ?? "#111" }]}>
      {/* Status Summary */}
      {statusSummary && (
        <View
          style={[
            styles.summaryBar,
            { backgroundColor: colors.surface?.cardSoft ?? "#1a1a1a" },
          ]}
        >
          <Pressable
            onPress={() => setStatusFilter("all")}
            style={[
              styles.summaryItem,
              statusFilter === "all" && styles.summaryItemActive,
            ]}
          >
            <Text style={[styles.summaryCount, { color: colors.text?.primary ?? "#fff" }]}>
              {injuries.length}
            </Text>
            <Text style={[styles.summaryLabel, { color: colors.text?.muted ?? "#888" }]}>
              All
            </Text>
          </Pressable>

          <Pressable
            onPress={() => setStatusFilter("out")}
            style={[
              styles.summaryItem,
              statusFilter === "out" && styles.summaryItemActive,
            ]}
          >
            <Text style={[styles.summaryCount, { color: "#ef4444" }]}>
              {statusSummary.out}
            </Text>
            <Text style={[styles.summaryLabel, { color: colors.text?.muted ?? "#888" }]}>
              Out
            </Text>
          </Pressable>

          <Pressable
            onPress={() => setStatusFilter("questionable")}
            style={[
              styles.summaryItem,
              statusFilter === "questionable" && styles.summaryItemActive,
            ]}
          >
            <Text style={[styles.summaryCount, { color: "#eab308" }]}>
              {statusSummary.questionable + statusSummary.doubtful}
            </Text>
            <Text style={[styles.summaryLabel, { color: colors.text?.muted ?? "#888" }]}>
              GTD
            </Text>
          </Pressable>

          <Pressable
            onPress={() => setStatusFilter("probable")}
            style={[
              styles.summaryItem,
              statusFilter === "probable" && styles.summaryItemActive,
            ]}
          >
            <Text style={[styles.summaryCount, { color: "#22c55e" }]}>
              {statusSummary.probable + statusSummary.day_to_day}
            </Text>
            <Text style={[styles.summaryLabel, { color: colors.text?.muted ?? "#888" }]}>
              Prob
            </Text>
          </Pressable>
        </View>
      )}

      {/* View Toggle */}
      <View style={styles.toggleRow}>
        <Pressable
          onPress={() => setViewMode("byTeam")}
          style={[
            styles.toggleButton,
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
              styles.toggleText,
              {
                color:
                  viewMode === "byTeam"
                    ? "#fff"
                    : colors.text?.muted ?? "#888",
              },
            ]}
          >
            By Team
          </Text>
        </Pressable>

        <Pressable
          onPress={() => setViewMode("all")}
          style={[
            styles.toggleButton,
            {
              backgroundColor:
                viewMode === "all"
                  ? colors.accent?.primary ?? "#3b82f6"
                  : colors.surface?.cardSoft ?? "#222",
            },
          ]}
        >
          <Text
            style={[
              styles.toggleText,
              {
                color:
                  viewMode === "all" ? "#fff" : colors.text?.muted ?? "#888",
              },
            ]}
          >
            All Players
          </Text>
        </Pressable>
      </View>

      {/* Error State */}
      {error && (
        <Text style={[styles.errorText, { color: colors.accent?.danger ?? "#ef4444" }]}>
          Error: {error}
        </Text>
      )}

      {/* Content */}
      {viewMode === "byTeam" ? (
        <ScrollView
          contentContainerStyle={styles.listContent}
          refreshControl={
            <RefreshControl refreshing={loading} onRefresh={refresh} />
          }
        >
          {filteredByTeam.map((team) => (
            <TeamInjuriesSection
              key={team.team}
              teamData={team}
              defaultExpanded={filteredByTeam.length <= 5}
            />
          ))}
          {filteredByTeam.length === 0 && !loading && (
            <Text style={[styles.emptyText, { color: colors.text?.muted ?? "#888" }]}>
              No injuries found
            </Text>
          )}
        </ScrollView>
      ) : (
        <FlatList
          contentContainerStyle={styles.listContent}
          data={filteredInjuries}
          keyExtractor={(item) =>
            String(item.injury_id ?? `${item.player_id}-${item.team_id}`)
          }
          renderItem={({ item }) => <InjuryCard injury={item} />}
          refreshControl={
            <RefreshControl refreshing={loading} onRefresh={refresh} />
          }
          ListEmptyComponent={
            !loading ? (
              <Text style={[styles.emptyText, { color: colors.text?.muted ?? "#888" }]}>
                No injuries found
              </Text>
            ) : null
          }
        />
      )}
    </View>
  );
}

const styles = StyleSheet.create({
  container: {
    flex: 1,
  },
  summaryBar: {
    flexDirection: "row",
    justifyContent: "space-around",
    paddingVertical: 12,
    paddingHorizontal: 8,
  },
  summaryItem: {
    alignItems: "center",
    paddingHorizontal: 12,
    paddingVertical: 4,
    borderRadius: 8,
  },
  summaryItemActive: {
    backgroundColor: "rgba(255,255,255,0.1)",
  },
  summaryCount: {
    fontSize: 20,
    fontWeight: "700",
  },
  summaryLabel: {
    fontSize: 11,
    marginTop: 2,
  },
  toggleRow: {
    flexDirection: "row",
    paddingHorizontal: 12,
    paddingVertical: 8,
    gap: 8,
  },
  toggleButton: {
    flex: 1,
    paddingVertical: 8,
    borderRadius: 8,
    alignItems: "center",
  },
  toggleText: {
    fontSize: 13,
    fontWeight: "600",
  },
  listContent: {
    padding: 12,
    paddingBottom: 40,
  },
  errorText: {
    padding: 12,
    textAlign: "center",
  },
  emptyText: {
    textAlign: "center",
    marginTop: 40,
    fontSize: 14,
  },
});
