import { ScrollView, Text, View, ActivityIndicator } from "react-native";
import { Stack } from "expo-router";
import { useEffect, useMemo, useState } from "react";

import { useTheme } from "@/store/useTheme";
import { usePgaCourses } from "@/hooks/pga/usePgaCourses";
import { usePgaQuery } from "@/hooks/pga/usePgaQuery";
import { MetricCard } from "@/components/pga/MetricCard";
import { SearchPicker, SearchItem } from "@/components/pga/SearchPicker";
import { PgaCourse, PgaCourseComp, PgaCourseFitRow } from "@/types/pga";

type Response = {
  course: PgaCourse | null;
  comps: PgaCourseComp[];
  players: PgaCourseFitRow[];
};

export default function CourseFitScreen() {
  const { colors } = useTheme();
  const [search, setSearch] = useState("");
  const [selected, setSelected] = useState<SearchItem | null>(null);

  const { data: courseData } = usePgaCourses({ search });
  const courseItems = useMemo(
    () =>
      (courseData?.data || []).map((course) => ({
        id: course.id,
        label: course.name,
        subLabel: [course.city, course.state, course.country]
          .filter(Boolean)
          .join(", "),
      })),
    [courseData]
  );

  useEffect(() => {
    if (!selected && courseItems.length > 0) {
      setSelected(courseItems[0]);
    }
  }, [courseItems, selected]);

  const { data, loading, error } = usePgaQuery<Response>(
    "/pga/analytics/course-fit",
    {
      course_id: selected?.id,
      seasons_back: 2,
      last_n: 20,
      min_events: 2,
    },
    !!selected
  );

  return (
    <>
      <Stack.Screen
        options={{
          title: "Course Fit Model",
          headerStyle: { backgroundColor: colors.surface.screen },
          headerTintColor: colors.text.primary,
        }}
      />
      <ScrollView
        style={{ flex: 1, backgroundColor: colors.surface.screen }}
        contentContainerStyle={{ padding: 16, paddingBottom: 40 }}
      >
        <SearchPicker
          title="Course"
          placeholder="Search courses..."
          query={search}
          onQueryChange={setSearch}
          items={courseItems}
          selectedId={selected?.id}
          onSelect={setSelected}
          helperText="Select a course to evaluate player fit. Lower fit score = stronger fit (better expected finish)."
        />

        {loading ? (
          <View style={{ padding: 20, alignItems: "center" }}>
            <ActivityIndicator color={colors.accent.primary} />
          </View>
        ) : null}

        {error ? (
          <Text style={{ color: colors.text.danger, marginTop: 12 }}>
            {error}
          </Text>
        ) : null}

        {data?.course ? (
          <Text style={{ color: colors.text.primary, fontWeight: "700" }}>
            {data.course.name}
          </Text>
        ) : null}
        {data?.comps?.length ? (
          <Text style={{ color: colors.text.muted, fontSize: 12, marginTop: 4 }}>
            Comp courses: {data.comps.map((comp) => comp.course.name).join(", ")}
          </Text>
        ) : null}

        <Text style={{ color: colors.text.muted, marginTop: 8 }}>Fit model = 70% target-course average finish + 30% comp-course average finish. Lower is better.</Text>

        <View style={{ marginTop: 12 }}>
          {(data?.players || []).slice(0, 25).map((row) => (
            <MetricCard
              key={row.player_id}
              title={row.player.display_name}
              subtitle={`Course events: ${row.course_events} | Comp events: ${row.comp_events}`}
              metrics={[
                { label: "Course Avg", value: row.course_avg_finish },
                { label: "Comp Avg", value: row.comp_avg_finish },
                { label: "Fit Score", value: row.course_fit_score },
              ]}
            />
          ))}
        </View>
      </ScrollView>
    </>
  );
}
