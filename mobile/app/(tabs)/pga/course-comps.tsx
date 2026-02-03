import { ScrollView, View, Text, ActivityIndicator } from "react-native";
import { Stack } from "expo-router";
import { useEffect, useMemo, useState } from "react";

import { useTheme } from "@/store/useTheme";
import { usePgaCourses } from "@/hooks/pga/usePgaCourses";
import { usePgaQuery } from "@/hooks/pga/usePgaQuery";
import { MetricCard } from "@/components/pga/MetricCard";
import { SearchPicker, SearchItem } from "@/components/pga/SearchPicker";
import { PgaCourse, PgaCourseComp } from "@/types/pga";

type Response = {
  course: PgaCourse | null;
  comps: PgaCourseComp[];
};

export default function CourseCompsScreen() {
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
    "/pga/analytics/course-comps",
    { course_id: selected?.id, limit: 8 },
    !!selected
  );

  return (
    <>
      <Stack.Screen
        options={{
          title: "Comp Courses",
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

        {(data?.comps || []).map((comp) => (
          <MetricCard
            key={comp.course.id}
            title={comp.course.name}
            subtitle={[comp.course.city, comp.course.state]
              .filter(Boolean)
              .join(", ")}
            metrics={[
              { label: "Similarity", value: comp.similarity },
              { label: "Par", value: comp.course.par },
              { label: "Yardage", value: comp.course.yardage },
            ]}
          />
        ))}
      </ScrollView>
    </>
  );
}
