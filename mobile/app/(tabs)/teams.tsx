// app/(tabs)/teams.tsx
import { View, Text, FlatList } from "react-native";
import { useEffect, useState } from "react";
import Constants from "expo-constants";

import { TeamSeasonStats } from "@/types/teamStats";
import { useSortableData } from "@/hooks/useSortableData";
import { SortableHeader } from "@/components/table/SortableHeader";
import { useTheme } from "@/store/useTheme";

const API = Constants.expoConfig?.extra?.API_URL!;

export default function TeamsScreen() {
  const { colors } = useTheme();
  const [data, setData] = useState<TeamSeasonStats[]>([]);

  useEffect(() => {
    fetch(`${API}/teams/season-stats`)
      .then(r => r.json())
      .then(setData)
      .catch(console.error);
  }, []);

  const {
    sortedData,
    sortKey,
    direction,
    toggleSort,
  } = useSortableData(data, "pts");

  return (
    <View style={{ flex: 1, padding: 12 }}>
      {/* HEADER */}
      <View
        style={{
          flexDirection: "row",
          borderBottomWidth: 1,
          borderColor: colors.border.subtle,
          paddingBottom: 6,
        }}
      >
        <SortableHeader
          label="Team"
          width={90}
          active={sortKey === "team_abbr"}
          direction={direction}
          onPress={() => toggleSort("team_abbr")}
        />
        <SortableHeader
          label="PTS"
          active={sortKey === "pts"}
          direction={direction}
          onPress={() => toggleSort("pts")}
        />
        <SortableHeader
          label="FG%"
          active={sortKey === "fg_pct"}
          direction={direction}
          onPress={() => toggleSort("fg_pct")}
        />
        <SortableHeader
          label="3P%"
          active={sortKey === "fg3_pct"}
          direction={direction}
          onPress={() => toggleSort("fg3_pct")}
        />
        <SortableHeader
          label="REB"
          active={sortKey === "reb"}
          direction={direction}
          onPress={() => toggleSort("reb")}
        />
        <SortableHeader
          label="AST"
          active={sortKey === "ast"}
          direction={direction}
          onPress={() => toggleSort("ast")}
        />
      </View>

      {/* ROWS */}
      <FlatList
        data={sortedData}
        keyExtractor={i => String(i.team_id)}
        renderItem={({ item }) => (
          <View
            style={{
              flexDirection: "row",
              paddingVertical: 6,
              borderBottomWidth: 0.5,
              borderColor: colors.border.subtle,
            }}
          >
            <Text style={{ width: 90 }}>{item.team_abbr}</Text>
            <Text style={{ width: 70 }}>{item.pts.toFixed(1)}</Text>
            <Text style={{ width: 70 }}>{(item.fg_pct * 100).toFixed(1)}</Text>
            <Text style={{ width: 70 }}>{(item.fg3_pct * 100).toFixed(1)}</Text>
            <Text style={{ width: 70 }}>{item.reb.toFixed(1)}</Text>
            <Text style={{ width: 70 }}>{item.ast.toFixed(1)}</Text>
          </View>
        )}
      />
    </View>
  );
}