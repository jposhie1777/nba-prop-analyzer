// app/(tabs)/teams.tsx
import { View } from "react-native";
import { SafeAreaView } from "react-native-safe-area-context";
import { useEffect, useState } from "react";
import Constants from "expo-constants";

import { AutoSortableTable } from "@/components/table/AutoSortableTable";
import { schemaToColumns } from "@/utils/schemaToColumns";
import { ColumnSchema } from "@/types/schema";

const API = Constants.expoConfig?.extra?.API_URL!;

export default function TeamsScreen() {
  const [data, setData] = useState<any[]>([]);
  const [schema, setSchema] = useState<ColumnSchema[]>([]);

  useEffect(() => {
    fetch(`${API}/teams/season-stats`)
      .then(r => r.json())
      .then(setData);

    fetch(`${API}/teams/season-stats/schema`)
      .then(r => r.json())
      .then(setSchema);
  }, []);

  if (!schema.length) return null;

  const columns = schemaToColumns(schema);

  return (
    <SafeAreaView style={{ flex: 1 }}>
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