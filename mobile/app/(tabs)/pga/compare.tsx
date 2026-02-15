import { ScrollView, View, Text, Pressable, ActivityIndicator, StyleSheet } from "react-native";
import { Ionicons } from "@expo/vector-icons";
import { Stack } from "expo-router";
import { useEffect, useMemo, useState } from "react";

import { useTheme } from "@/store/useTheme";
import { usePgaPlayers } from "@/hooks/pga/usePgaPlayers";
import { usePgaCourses } from "@/hooks/pga/usePgaCourses";
import { usePgaTournaments } from "@/hooks/pga/usePgaTournaments";
import { usePgaQuery } from "@/hooks/pga/usePgaQuery";
import { SearchPicker, SearchItem } from "@/components/pga/SearchPicker";
import { MetricCard } from "@/components/pga/MetricCard";
import { PgaCompareResponse, PgaRoundScore } from "@/types/pga";

type CompareCount = 2 | 3;

const formatPct = (value?: number | null) =>
  value == null ? "—" : `${(value * 100).toFixed(1)}%`;

const formatNum = (value?: number | null) =>
  value == null ? "—" : value.toFixed(2);

const formatPar = (value?: number | null) => {
  if (value == null) return "—";
  if (value === 0) return "E";
  return value > 0 ? `+${value}` : `${value}`;
};

const formatRoundScoreValue = (round: PgaRoundScore) => {
  if (round.par_relative_score != null) {
    return formatPar(round.par_relative_score);
  }
  if (round.round_score != null) {
    return `${round.round_score}`;
  }
  if (round.total_score != null) {
    return `${round.total_score}`;
  }
  return "—";
};

const formatRoundScores = (rounds?: PgaRoundScore[] | null) => {
  if (!rounds || rounds.length === 0) return "—";
  const ordered = rounds
    .filter((round) => round.round_number != null)
    .sort((a, b) => a.round_number - b.round_number);
  if (ordered.length === 0) return "—";
  return ordered
    .map((round) => `R${round.round_number}: ${formatRoundScoreValue(round)}`)
    .join(" • ");
};

export default function CompareScreen() {
  const { colors } = useTheme();
  const [compareCount, setCompareCount] = useState<CompareCount>(2);

  const [searchA, setSearchA] = useState("");
  const [searchB, setSearchB] = useState("");
  const [searchC, setSearchC] = useState("");
  const [playerA, setPlayerA] = useState<SearchItem | null>(null);
  const [playerB, setPlayerB] = useState<SearchItem | null>(null);
  const [playerC, setPlayerC] = useState<SearchItem | null>(null);

  const [tournamentSearch, setTournamentSearch] = useState("");
  const [selectedTournament, setSelectedTournament] = useState<SearchItem | null>(null);

  const [courseSearch, setCourseSearch] = useState("");
  const [selectedCourse, setSelectedCourse] = useState<SearchItem | null>(null);

  const { data: playersA } = usePgaPlayers({ search: searchA, active: true });
  const { data: playersB } = usePgaPlayers({ search: searchB, active: true });
  const { data: playersC } = usePgaPlayers({ search: searchC, active: true });
  const { data: tournaments } = usePgaTournaments({
    season: new Date().getFullYear(),
  });
  const { data: courses } = usePgaCourses({ search: courseSearch });

  const itemsA = useMemo(
    () =>
      (playersA?.data || []).map((player) => ({
        id: player.id,
        label: player.display_name,
        subLabel: player.country ?? "",
      })),
    [playersA]
  );
  const itemsB = useMemo(
    () =>
      (playersB?.data || []).map((player) => ({
        id: player.id,
        label: player.display_name,
        subLabel: player.country ?? "",
      })),
    [playersB]
  );
  const itemsC = useMemo(
    () =>
      (playersC?.data || []).map((player) => ({
        id: player.id,
        label: player.display_name,
        subLabel: player.country ?? "",
      })),
    [playersC]
  );

  const upcomingTournaments = useMemo(() => {
    const now = Date.now();
    return (tournaments?.data || [])
      .filter((tournament) => {
        const startTime = tournament.start_date
          ? new Date(tournament.start_date).getTime()
          : Number.NaN;
        const endTime = tournament.end_date
          ? new Date(tournament.end_date).getTime()
          : Number.NaN;

        const hasUpcomingStart = !Number.isNaN(startTime) && startTime >= now;
        const isInProgress =
          !Number.isNaN(startTime) &&
          !Number.isNaN(endTime) &&
          startTime <= now &&
          endTime >= now;

        return hasUpcomingStart || isInProgress;
      })
      .sort((a, b) => {
        const aTime = a.start_date ? new Date(a.start_date).getTime() : 0;
        const bTime = b.start_date ? new Date(b.start_date).getTime() : 0;
        return aTime - bTime;
      })
      .slice(0, 3);
  }, [tournaments]);

  const tournamentItems = useMemo(() => {
    const search = tournamentSearch.trim().toLowerCase();
    const pool = search ? tournaments?.data || [] : upcomingTournaments;
    return pool
      .filter((tournament) =>
        search ? tournament.name.toLowerCase().includes(search) : true
      )
      .map((tournament) => ({
        id: tournament.id,
        label: tournament.name,
        subLabel: tournament.course_name ?? "",
      }));
  }, [tournaments, tournamentSearch, upcomingTournaments]);

  const upcomingCourseItems = useMemo(() => {
    const courseLookup = new Map(
      (courses?.data || []).map((course) => [course.name.toLowerCase(), course])
    );
    const items: SearchItem[] = [];
    const seen = new Set<number>();

    upcomingTournaments.forEach((tournament) => {
      const coursesInTournament = tournament.courses?.length
        ? tournament.courses.map((entry) => entry.course)
        : [];

      if (coursesInTournament.length > 0) {
        coursesInTournament.forEach((course) => {
          if (!course?.id || seen.has(course.id)) return;
          seen.add(course.id);
          items.push({
            id: course.id,
            label: course.name,
            subLabel: [course.city, course.state].filter(Boolean).join(", "),
          });
        });
      } else if (tournament.course_name) {
        const match = courseLookup.get(tournament.course_name.toLowerCase());
        if (match && !seen.has(match.id)) {
          seen.add(match.id);
          items.push({
            id: match.id,
            label: match.name,
            subLabel: [match.city, match.state].filter(Boolean).join(", "),
          });
        }
      }
    });

    return items.slice(0, 3);
  }, [courses, upcomingTournaments]);

  const courseItems = useMemo(() => {
    const search = courseSearch.trim();
    if (!search) {
      return upcomingCourseItems;
    }
    return (courses?.data || []).map((course) => ({
      id: course.id,
      label: course.name,
      subLabel: [course.city, course.state].filter(Boolean).join(", "),
    }));
  }, [courseSearch, courses, upcomingCourseItems]);

  useEffect(() => {
    if (!playerA && itemsA.length > 0) {
      setPlayerA(itemsA[0]);
    }
  }, [itemsA, playerA]);

  useEffect(() => {
    if (!playerB && itemsB.length > 0) {
      setPlayerB(itemsB[0]);
    }
  }, [itemsB, playerB]);

  useEffect(() => {
    if (compareCount === 2) {
      setPlayerC(null);
      return;
    }
    if (!playerC && itemsC.length > 0) {
      setPlayerC(itemsC[0]);
    }
  }, [compareCount, itemsC, playerC]);

  const playerIds = useMemo(() => {
    const ids = [playerA?.id, playerB?.id, playerC?.id].filter(
      (value): value is number => typeof value === "number"
    );
    return compareCount === 2 ? ids.slice(0, 2) : ids.slice(0, 3);
  }, [compareCount, playerA, playerB, playerC]);

  const { data, loading, error } = usePgaQuery<PgaCompareResponse>(
    "/pga/analytics/compare",
    {
      player_ids: playerIds,
      course_id: selectedCourse?.id,
      tournament_id: selectedTournament?.id,
    },
    playerIds.length >= 2
  );

  const recommendation = data?.recommendation;
  const recommendedPlayer = useMemo(() => {
    if (!recommendation) return null;
    return data?.players.find(
      (row) => row.player_id === recommendation.player_id
    );
  }, [data, recommendation]);

  return (
    <>
      <Stack.Screen
        options={{
          title: "Pairings Compare",
          headerStyle: { backgroundColor: colors.surface.screen },
          headerTintColor: colors.text.primary,
        }}
      />
      <ScrollView
        style={{ flex: 1, backgroundColor: colors.surface.screen }}
        contentContainerStyle={{ padding: 16, paddingBottom: 40 }}
      >
        <Text style={cmpStyles.sectionLabel}>
          <Text style={{ color: colors.text.muted, fontSize: 11, fontWeight: "600", letterSpacing: 1 }}>
            COMPARE MODE
          </Text>
        </Text>
        <View style={cmpStyles.toggleRow}>
          {[2, 3].map((count) => {
            const isActive = compareCount === count;
            return (
              <Pressable
                key={count}
                onPress={() => setCompareCount(count as CompareCount)}
                style={[
                  cmpStyles.toggleBtn,
                  {
                    borderColor: isActive
                      ? colors.accent.primary
                      : colors.border.subtle,
                    backgroundColor: isActive
                      ? colors.state.selected
                      : colors.surface.card,
                  },
                ]}
              >
                <Ionicons
                  name={count === 2 ? "people-outline" : "people"}
                  size={16}
                  color={isActive ? colors.accent.primary : colors.text.muted}
                  style={{ marginRight: 6 }}
                />
                <Text
                  style={{
                    color: isActive ? colors.accent.primary : colors.text.primary,
                    fontWeight: "600",
                    fontSize: 14,
                  }}
                >
                  {count} Players
                </Text>
              </Pressable>
            );
          })}
        </View>

        <SearchPicker
          title="Player A"
          placeholder="Search player..."
          query={searchA}
          onQueryChange={setSearchA}
          items={itemsA}
          selectedId={playerA?.id}
          onSelect={setPlayerA}
        />
        <SearchPicker
          title="Player B"
          placeholder="Search player..."
          query={searchB}
          onQueryChange={setSearchB}
          items={itemsB}
          selectedId={playerB?.id}
          onSelect={setPlayerB}
        />
        {compareCount === 3 ? (
          <SearchPicker
            title="Player C"
            placeholder="Search player..."
            query={searchC}
            onQueryChange={setSearchC}
            items={itemsC}
            selectedId={playerC?.id}
            onSelect={setPlayerC}
          />
        ) : null}

        <SearchPicker
          title="Tournament (optional)"
          placeholder="Filter tournaments..."
          query={tournamentSearch}
          onQueryChange={setTournamentSearch}
          items={tournamentItems}
          selectedId={selectedTournament?.id}
          onSelect={setSelectedTournament}
          helperText="Boosts players with strong history at the event."
        />
        {selectedTournament ? (
          <Pressable
            onPress={() => setSelectedTournament(null)}
            style={cmpStyles.clearBtn}
          >
            <Ionicons name="close-circle" size={14} color={colors.accent.primary} />
            <Text style={{ color: colors.accent.primary, marginLeft: 4, fontSize: 13, fontWeight: "600" }}>
              Clear tournament
            </Text>
          </Pressable>
        ) : null}

        <SearchPicker
          title="Course (optional)"
          placeholder="Search course..."
          query={courseSearch}
          onQueryChange={setCourseSearch}
          items={courseItems}
          selectedId={selectedCourse?.id}
          onSelect={setSelectedCourse}
          helperText="Uses course fit + comp courses."
        />
        {selectedCourse ? (
          <Pressable
            onPress={() => setSelectedCourse(null)}
            style={cmpStyles.clearBtn}
          >
            <Ionicons name="close-circle" size={14} color={colors.accent.primary} />
            <Text style={{ color: colors.accent.primary, marginLeft: 4, fontSize: 13, fontWeight: "600" }}>
              Clear course
            </Text>
          </Pressable>
        ) : null}

        {loading ? (
          <View style={{ padding: 20, alignItems: "center" }}>
            <ActivityIndicator color={colors.accent.primary} />
          </View>
        ) : null}

        {error ? (
          <Text style={{ color: colors.text.danger, marginTop: 12 }}>{error}</Text>
        ) : null}

        {recommendation && recommendedPlayer ? (
          <MetricCard
            title="Recommendation"
            subtitle={recommendation.label}
            metrics={[
              {
                label: "Best Play",
                value: recommendedPlayer.player.display_name,
              },
              { label: "Edge", value: formatNum(recommendation.edge) },
              {
                label: "Reasons",
                value: recommendation.reasons.join(" • "),
              },
            ]}
          />
        ) : null}

        {(data?.players || []).map((row) => (
          <MetricCard
            key={row.player_id}
            title={`${row.rank}. ${row.player.display_name}`}
            subtitle={`Score: ${formatNum(row.score)}`}
            imageUrl={row.player.player_image_url}
            metrics={[
              { label: "Form Score", value: formatNum(row.metrics.form_score) },
              {
                label: "Course Fit",
                value: row.metrics.course_fit_score ?? "\u2014",
              },
              {
                label: "H2H Win %",
                value: formatPct(row.metrics.head_to_head_win_rate),
              },
              { label: "Top 10", value: formatPct(row.metrics.top10_prob) },
              { label: "Top 20", value: formatPct(row.metrics.top20_prob) },
              {
                label: "Tournament Bonus",
                value: formatPct(row.metrics.tournament_bonus),
              },
              {
                label: "Round Scores",
                value: formatRoundScores(row.metrics.round_scores),
              },
            ]}
          />
        ))}
      </ScrollView>
    </>
  );
}

const cmpStyles = StyleSheet.create({
  sectionLabel: {
    marginBottom: 4,
  },
  toggleRow: {
    flexDirection: "row",
    gap: 10,
    marginTop: 8,
    marginBottom: 16,
  },
  toggleBtn: {
    flexDirection: "row",
    alignItems: "center",
    paddingHorizontal: 14,
    paddingVertical: 9,
    borderRadius: 12,
    borderWidth: 1,
  },
  clearBtn: {
    flexDirection: "row",
    alignItems: "center",
    marginBottom: 12,
  },
});
