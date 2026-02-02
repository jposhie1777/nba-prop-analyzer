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
import type { BadLine } from "@/components/bad-lines/BadLineCard";
import { GameBadLinesSection } from "@/components/bad-lines/GameBadLinesSection";
import { fetchBadLines } from "@/lib/apiMaster";


/* ======================================================
   CONSTANTS
====================================================== */
const SCORE_FILTERS = [1.25, 1.5, 2.0];

type GameGroup = {
  key: string;
  gameId?: number;
  awayTeamAbbr: string;
  homeTeamAbbr: string;
  lines: BadLine[];
};

/* ======================================================
   SCREEN
====================================================== */
export default function BadLinesScreen() {
  const colors = useTheme((s) => s.colors);
  const styles = useMemo(() => makeStyles(colors), [colors]);

  const [minScore, setMinScore] = useState(1.25);

  const {
    data,
    isLoading,
    isFetching,
    refetch,
  } = useQuery({
    queryKey: ["bad-lines", minScore],
    queryFn: () => fetchBadLines({ min_score: minScore }),
    refetchInterval: 60_000,
  });

  const badLines = data?.bad_lines ?? [];
  const badLinesByGame = useMemo<GameGroup[]>(() => {
    const groups = new Map<string, GameGroup>();

    badLines.forEach((line) => {
      const awayTeamAbbr = line.away_team_abbr ?? "TBD";
      const homeTeamAbbr = line.home_team_abbr ?? "TBD";
      const key =
        line.game_id != null
          ? String(line.game_id)
          : `${awayTeamAbbr}-${homeTeamAbbr}`;

      const existing = groups.get(key);
      if (existing) {
        existing.lines.push(line);
        return;
      }

      groups.set(key, {
        key,
        gameId: line.game_id,
        awayTeamAbbr,
        homeTeamAbbr,
        lines: [line],
      });
    });

    const grouped = Array.from(groups.values());
    grouped.forEach((group) => {
      group.lines.sort((a, b) => b.bad_line_score - a.bad_line_score);
    });

    return grouped;
  }, [badLines]);

  if (isLoading) {
    return (
      <View style={{ flex: 1, justifyContent: "center" }}>
        <ActivityIndicator size="large" />
      </View>
    );
  }

  return (
    <View style={styles.root}>
      {/* ==================================================
         HEADER
      ================================================== */}
      <View style={styles.header}>
        <Text style={styles.title}>Bad Lines</Text>
        <Text style={styles.subtitle}>
          {badLines.length} opportunities · live scan
        </Text>
      </View>

      {/* ==================================================
         FILTERS
      ================================================== */}
      <View style={styles.filters}>
        {SCORE_FILTERS.map((score) => {
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
        })}
      </View>

      {/* ==================================================
         EMPTY STATE
      ================================================== */}
      {badLines.length === 0 ? (
        <View style={styles.empty}>
          <Text style={styles.emptyTitle}>
            No bad lines right now
          </Text>
          <Text style={styles.emptySubtitle}>
            Markets are tight — check back soon
          </Text>
        </View>
      ) : (
        /* ==================================================
           LIST
        ================================================== */
        <FlatList
          data={badLinesByGame}
          keyExtractor={(item) => item.key}
          renderItem={({ item }) => (
            <GameBadLinesSection
              gameId={item.gameId}
              awayTeamAbbr={item.awayTeamAbbr}
              homeTeamAbbr={item.homeTeamAbbr}
              lines={item.lines}
              defaultExpanded={false}
            />
          )}
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

    listContent: {
      paddingHorizontal: 12,
      paddingTop: 8,
      paddingBottom: 24,
    },

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
