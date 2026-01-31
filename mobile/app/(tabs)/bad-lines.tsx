import {
  View,
  Text,
  FlatList,
  Pressable,
  StyleSheet,
  ActivityIndicator,
} from "react-native";
import { useMemo, useState } from "react";
import { useQuery } from "@tanstack/react-query";

import { useTheme } from "@/store/useTheme";
import BadLineCard from "@/components/bad-lines/BadLineCard";
import LiveBadLineCard from "@/components/bad-lines/LiveBadLineCard";
import { fetchBadLines, fetchLiveBadLines } from "@/lib/apiMaster";


/* ======================================================
   CONSTANTS
====================================================== */
const SCORE_FILTERS = [1.25, 1.5, 2.0];
const EDGE_FILTERS = [0.10, 0.15, 0.25];

type TabType = "pre-live" | "live";

/* ======================================================
   SCREEN
====================================================== */
export default function BadLinesScreen() {
  const colors = useTheme((s) => s.colors);
  const styles = useMemo(() => makeStyles(colors), [colors]);

  const [activeTab, setActiveTab] = useState<TabType>("pre-live");
  const [minScore, setMinScore] = useState(1.25);
  const [minEdge, setMinEdge] = useState(0.15);

  // Pre-live bad lines query
  const preLiveQuery = useQuery({
    queryKey: ["bad-lines", "pre-live", minScore],
    queryFn: () => fetchBadLines({ min_score: minScore }),
    refetchInterval: 60_000,
    enabled: activeTab === "pre-live",
  });

  // Live bad lines query
  const liveQuery = useQuery({
    queryKey: ["bad-lines", "live", minEdge],
    queryFn: () => fetchLiveBadLines({ min_edge: minEdge }),
    refetchInterval: 20_000,
    enabled: activeTab === "live",
  });

  const isLoading = activeTab === "pre-live"
    ? preLiveQuery.isLoading
    : liveQuery.isLoading;

  const isFetching = activeTab === "pre-live"
    ? preLiveQuery.isFetching
    : liveQuery.isFetching;

  const refetch = activeTab === "pre-live"
    ? preLiveQuery.refetch
    : liveQuery.refetch;

  const preLiveLines = preLiveQuery.data?.bad_lines ?? [];
  const liveLines = liveQuery.data?.bad_lines ?? [];

  const currentLines = activeTab === "pre-live" ? preLiveLines : liveLines;

  return (
    <View style={styles.root}>
      {/* ==================================================
         HEADER
      ================================================== */}
      <View style={styles.header}>
        <Text style={styles.title}>Bad Lines</Text>
        <Text style={styles.subtitle}>
          {currentLines.length} opportunities
        </Text>
      </View>

      {/* ==================================================
         TAB TOGGLE
      ================================================== */}
      <View style={styles.tabContainer}>
        <Pressable
          onPress={() => setActiveTab("pre-live")}
          style={[
            styles.tab,
            activeTab === "pre-live" && styles.tabActive,
          ]}
        >
          <Text
            style={[
              styles.tabText,
              activeTab === "pre-live" && styles.tabTextActive,
            ]}
          >
            Pre-Live
          </Text>
          <View style={styles.tabBadge}>
            <Text style={styles.tabBadgeText}>{preLiveLines.length}</Text>
          </View>
        </Pressable>

        <Pressable
          onPress={() => setActiveTab("live")}
          style={[
            styles.tab,
            activeTab === "live" && styles.tabActive,
          ]}
        >
          {activeTab === "live" && (
            <View style={styles.liveDot} />
          )}
          <Text
            style={[
              styles.tabText,
              activeTab === "live" && styles.tabTextActive,
            ]}
          >
            Live
          </Text>
          <View style={styles.tabBadge}>
            <Text style={styles.tabBadgeText}>{liveLines.length}</Text>
          </View>
        </Pressable>
      </View>

      {/* ==================================================
         FILTERS
      ================================================== */}
      <View style={styles.filters}>
        {activeTab === "pre-live" ? (
          // Pre-live score filters
          SCORE_FILTERS.map((score) => {
            const active = minScore === score;
            return (
              <Pressable
                key={score}
                onPress={() => setMinScore(score)}
                style={[
                  styles.filterPill,
                  active && styles.filterPillActive,
                ]}
              >
                <Text
                  style={[
                    styles.filterText,
                    active && styles.filterTextActive,
                  ]}
                >
                  Score {score}+
                </Text>
              </Pressable>
            );
          })
        ) : (
          // Live edge filters
          EDGE_FILTERS.map((edge) => {
            const active = minEdge === edge;
            return (
              <Pressable
                key={edge}
                onPress={() => setMinEdge(edge)}
                style={[
                  styles.filterPill,
                  active && styles.filterPillActive,
                ]}
              >
                <Text
                  style={[
                    styles.filterText,
                    active && styles.filterTextActive,
                  ]}
                >
                  Edge {(edge * 100).toFixed(0)}%+
                </Text>
              </Pressable>
            );
          })
        )}
      </View>

      {/* ==================================================
         LOADING STATE
      ================================================== */}
      {isLoading ? (
        <View style={styles.loading}>
          <ActivityIndicator size="large" />
        </View>
      ) : currentLines.length === 0 ? (
        /* ==================================================
           EMPTY STATE
        ================================================== */
        <View style={styles.empty}>
          <Text style={styles.emptyTitle}>
            {activeTab === "pre-live"
              ? "No bad lines right now"
              : "No live edges detected"}
          </Text>
          <Text style={styles.emptySubtitle}>
            {activeTab === "pre-live"
              ? "Markets are tight - check back soon"
              : "Wait for games to start or lower the edge filter"}
          </Text>
        </View>
      ) : (
        /* ==================================================
           LIST
        ================================================== */
        <FlatList
          data={currentLines}
          keyExtractor={(x, i) =>
            activeTab === "pre-live"
              ? `${x.player_id}-${x.market}-${x.line_value}`
              : `live-${x.player_id}-${x.market}-${x.line_value}-${i}`
          }
          renderItem={({ item }) =>
            activeTab === "pre-live" ? (
              <BadLineCard line={item} />
            ) : (
              <LiveBadLineCard line={item} />
            )
          }
          refreshing={isFetching}
          onRefresh={refetch}
          contentContainerStyle={styles.listContent}
        />
      )}
    </View>
  );
}

/* ======================================================
   STYLES
====================================================== */
function makeStyles(colors: any) {
  return StyleSheet.create({
    root: {
      flex: 1,
      backgroundColor: colors.surface.screen,
    },

    header: {
      paddingHorizontal: 16,
      paddingTop: 12,
      paddingBottom: 8,
    },

    title: {
      fontSize: 22,
      fontWeight: "700",
      color: colors.text.primary,
    },

    subtitle: {
      marginTop: 4,
      fontSize: 13,
      color: colors.text.muted,
    },

    /* Tab styles */
    tabContainer: {
      flexDirection: "row",
      paddingHorizontal: 16,
      paddingBottom: 12,
      gap: 8,
    },

    tab: {
      flexDirection: "row",
      alignItems: "center",
      paddingVertical: 8,
      paddingHorizontal: 16,
      borderRadius: 20,
      backgroundColor: colors.surface.cardSoft,
      gap: 6,
    },

    tabActive: {
      backgroundColor: colors.surface.card,
      borderWidth: 1,
      borderColor: colors.accent.primary,
    },

    tabText: {
      fontSize: 14,
      fontWeight: "600",
      color: colors.text.muted,
    },

    tabTextActive: {
      color: colors.text.primary,
    },

    tabBadge: {
      backgroundColor: colors.surface.subtle,
      borderRadius: 10,
      paddingHorizontal: 8,
      paddingVertical: 2,
    },

    tabBadgeText: {
      fontSize: 12,
      fontWeight: "600",
      color: colors.text.muted,
    },

    liveDot: {
      width: 8,
      height: 8,
      borderRadius: 4,
      backgroundColor: colors.accent.success,
    },

    /* Filter styles */
    filters: {
      flexDirection: "row",
      paddingHorizontal: 12,
      paddingBottom: 8,
      gap: 8,
    },

    filterPill: {
      paddingVertical: 6,
      paddingHorizontal: 12,
      borderRadius: 16,
      backgroundColor: colors.surface.cardSoft,
    },

    filterPillActive: {
      backgroundColor: colors.accent.primary,
    },

    filterText: {
      fontSize: 12,
      fontWeight: "600",
      color: colors.text.primary,
    },

    filterTextActive: {
      color: colors.text.inverse,
    },

    /* List styles */
    listContent: {
      paddingBottom: 24,
    },

    loading: {
      flex: 1,
      justifyContent: "center",
      alignItems: "center",
    },

    /* Empty state */
    empty: {
      flex: 1,
      alignItems: "center",
      justifyContent: "center",
      paddingHorizontal: 24,
    },

    emptyTitle: {
      fontSize: 16,
      fontWeight: "600",
      color: colors.text.primary,
      marginBottom: 6,
    },

    emptySubtitle: {
      fontSize: 13,
      color: colors.text.muted,
      textAlign: "center",
    },
  });
}
