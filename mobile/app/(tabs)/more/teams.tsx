// app/(tabs)/more/teams.tsx
import { View } from "react-native";
import { SafeAreaView } from "react-native-safe-area-context";
import { useEffect, useState } from "react";
import Constants from "expo-constants";

import { AutoSortableTable } from "@/components/table/AutoSortableTable";
import { schemaToColumns } from "@/utils/schemaToColumns";
import { ColumnSchema } from "@/types/schema";
import { useTheme } from "@/store/useTheme";

const API = Constants.expoConfig?.extra?.API_URL!;

export default function TeamsScreen() {
  const { colors } = useTheme();

  const [data, setData] = useState<any[]>([]);
  const [schema, setSchema] = useState<ColumnSchema[]>([]);

  useEffect(() => {
    fetch(`${API}/teams/season-stats`)
      .then(r => r.json())
      .then(setData)
      .catch(console.error);

    fetch(`${API}/teams/season-stats/schema`)
      .then(r => r.json())
      .then(setSchema)
      .catch(console.error);
  }, []);

  // ⛔ Prevent invisible render
  if (!schema.length || !data.length) {
    return (
      <SafeAreaView
        style={{
          flex: 1,
          backgroundColor: colors.surface.screen,
        }}
      />
    );
  }

  const columns = schemaToColumns(schema);

  return (
    <SafeAreaView
      style={{
        flex: 1,
        backgroundColor: colors.surface.screen,
      }}
    >
      <View style={{ flex: 1, padding: 12 }}>
        <AutoSortableTable
          data={data}
          columns={columns}
          defaultSort="pts"
        />
      </View>
    </SafeAreaView>
  );
}