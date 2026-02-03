import { ScrollView, View, Text, ActivityIndicator } from "react-native";
import { Stack } from "expo-router";
import { useEffect, useMemo, useState } from "react";

import { useTheme } from "@/store/useTheme";
import { usePgaCourses } from "@/hooks/pga/usePgaCourses";
import { usePgaQuery } from "@/hooks/pga/usePgaQuery";
import { MetricCard } from "@/components/pga/MetricCard";
import { SearchPicker, SearchItem } from "@/components/pga/SearchPicker";
import { PgaCourseProfile } from "@/types/pga";

export default function CourseProfileScreen() {
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

  const { data, loading, error } = usePgaQuery<PgaCourseProfile>(
    "/pga/analytics/course-profile",
    { course_id: selected?.id },
    !!selected
  );

  return (
    <>
      <Stack.Screen
        options={{
          title: "Course Profile",
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
          <MetricCard
            title={data.course.name}
            subtitle={[data.course.city, data.course.state, data.course.country]
              .filter(Boolean)
              .join(", ")}
            metrics={[
              { label: "Par 3", value: data.summary.par3_count },
              { label: "Par 4", value: data.summary.par4_count },
              { label: "Par 5", value: data.summary.par5_count },
              { label: "Total Par", value: data.summary.total_par },
              { label: "Avg Yardage", value: data.summary.avg_yardage },
              { label: "Total Yardage", value: data.summary.total_yardage },
            ]}
          />
        ) : null}

        {(data?.holes || []).map((hole) => (
          <MetricCard
            key={hole.hole_number}
            title={`Hole ${hole.hole_number}`}
            metrics={[
              { label: "Par", value: hole.par },
              { label: "Yardage", value: hole.yardage },
            ]}
          />
        ))}
      </ScrollView>
    </>
  );
}
