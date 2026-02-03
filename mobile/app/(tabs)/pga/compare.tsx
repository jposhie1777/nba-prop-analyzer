import { ScrollView, View, Text, Pressable, ActivityIndicator } from "react-native";
import { Stack } from "expo-router";
import { useEffect, useMemo, useState } from "react";

import { useTheme } from "@/store/useTheme";
import { usePgaPlayers } from "@/hooks/pga/usePgaPlayers";
import { usePgaCourses } from "@/hooks/pga/usePgaCourses";
import { usePgaTournaments } from "@/hooks/pga/usePgaTournaments";
import { usePgaQuery } from "@/hooks/pga/usePgaQuery";
import { SearchPicker, SearchItem } from "@/components/pga/SearchPicker";
import { MetricCard } from "@/components/pga/MetricCard";
import { PgaCompareResponse } from "@/types/pga";

type CompareCount = 2 | 3;

const formatPct = (value?: number | null) =>
  value == null ? "—" : `${(value * 100).toFixed(1)}%`;

const formatNum = (value?: number | null) =>
  value == null ? "—" : value.toFixed(2);

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

  const { data: playersA } = usePgaPlayers({ search: searchA });
  const { data: playersB } = usePgaPlayers({ search: searchB });
  const { data: playersC } = usePgaPlayers({ search: searchC });
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

  const tournamentItems = useMemo(() => {
    const search = tournamentSearch.trim().toLowerCase();
    return (tournaments?.data || [])
      .filter((tournament) =>
        search ? tournament.name.toLowerCase().includes(search) : true
      )
      .map((tournament) => ({
        id: tournament.id,
        label: tournament.name,
        subLabel: tournament.course_name ?? "",
      }));
  }, [tournaments, tournamentSearch]);

  const courseItems = useMemo(
    () =>
      (courses?.data || []).map((course) => ({
        id: course.id,
        label: course.name,
        subLabel: [course.city, course.state].filter(Boolean).join(", "),
      })),
    [courses]
  );

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
        <Text style={{ color: colors.text.primary, fontWeight: "700" }}>
          Compare Mode
        </Text>
        <View style={{ flexDirection: "row", gap: 8, marginTop: 8 }}>
          {[2, 3].map((count) => {
            const isActive = compareCount === count;
            return (
              <Pressable
                key={count}
                onPress={() => setCompareCount(count as CompareCount)}
                style={{
                  paddingHorizontal: 12,
                  paddingVertical: 8,
                  borderRadius: 999,
                  borderWidth: 1,
                  borderColor: isActive
                    ? colors.accent.primary
                    : colors.border.subtle,
                  backgroundColor: isActive
                    ? colors.surface.cardSoft
                    : colors.surface.card,
                }}
              >
                <Text style={{ color: colors.text.primary, fontWeight: "700" }}>
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
          <Pressable onPress={() => setSelectedTournament(null)}>
            <Text style={{ color: colors.accent.primary, marginBottom: 12 }}>
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
          <Pressable onPress={() => setSelectedCourse(null)}>
            <Text style={{ color: colors.accent.primary, marginBottom: 12 }}>
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
            metrics={[
              { label: "Form Score", value: formatNum(row.metrics.form_score) },
              {
                label: "Course Fit",
                value: row.metrics.course_fit_score ?? "—",
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
            ]}
          />
        ))}
      </ScrollView>
    </>
  );
}
