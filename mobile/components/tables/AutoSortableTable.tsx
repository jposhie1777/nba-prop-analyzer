// components/table/AutoSortableTable.tsx
import { View, Text, FlatList, Pressable } from "react-native";
import { ColumnConfig } from "@/types/schema";
import { useSortableData } from "@/hooks/useSortableData";
import { useTheme } from "@/store/useTheme";

export function AutoSortableTable<T>({
  data,
  columns,
  defaultSort,
}: {
  data: T[];
  columns: ColumnConfig[];
  defaultSort: keyof T;
}) {
  const { colors } = useTheme();

  const {
    sortedData,
    sortKey,
    direction,
    toggleSort,
  } = useSortableData(data, defaultSort);

  return (
    <>
      {/* HEADER */}
      <View style={{ flexDirection: "row", paddingBottom: 6 }}>
        {columns.map(col => (
          <Pressable
            key={col.key}
            onPress={() => toggleSort(col.key as keyof T)}
            style={{ width: col.width }}
          >
            <Text
              style={{
                fontWeight: "600",
                color:
                  sortKey === col.key
                    ? colors.accent.primary
                    : colors.text.secondary,
              }}
            >
              {col.label}
              {sortKey === col.key
                ? direction === "asc"
                  ? " ▲"
                  : " ▼"
                : ""}
            </Text>
          </Pressable>
        ))}
      </View>

      {/* ROWS */}
      <FlatList
        data={sortedData}
        keyExtractor={(_, i) => String(i)}
        renderItem={({ item }) => (
          <View style={{ flexDirection: "row", paddingVertical: 6 }}>
            {columns.map(col => {
              const raw = (item as any)[col.key];
              const value = col.formatter
                ? col.formatter(raw)
                : raw;

              return (
                <Text
                  key={col.key}
                  style={{ width: col.width }}
                >
                  {value ?? "—"}
                </Text>
              );
            })}
          </View>
        )}
      />
    </>
  );
}