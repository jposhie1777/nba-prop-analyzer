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
    <View style={{ flex: 1 }}>
      {/* =========================
          HEADER
      ========================== */}
      <View
        style={{
          flexDirection: "row",
          paddingVertical: 8,
          borderBottomWidth: 1,
          borderColor: colors.border.subtle,
        }}
      >
        {columns.map(col => {
          const isActive = sortKey === col.key;

          return (
            <Pressable
              key={col.key}
              onPress={() => toggleSort(col.key as keyof T)}
              style={{ width: col.width }}
            >
              <Text
                style={{
                  fontWeight: "600",
                  fontSize: 12,
                  color: isActive
                    ? colors.accent.primary
                    : colors.text.secondary,
                }}
                numberOfLines={1}
              >
                {col.label}
                {isActive
                  ? direction === "asc"
                    ? " ▲"
                    : " ▼"
                  : ""}
              </Text>
            </Pressable>
          );
        })}
      </View>

      {/* =========================
          ROWS
      ========================== */}
      <FlatList
        data={sortedData}
        keyExtractor={(_, i) => String(i)}
        contentContainerStyle={{ paddingBottom: 24 }}
        showsVerticalScrollIndicator={false}
        renderItem={({ item }) => (
          <View
            style={{
              flexDirection: "row",
              paddingVertical: 6,
              borderBottomWidth: 0.5,
              borderColor: colors.border.subtle,
            }}
          >
            {columns.map(col => {
              const raw = (item as any)[col.key];
              const value = col.formatter
                ? col.formatter(raw)
                : raw;

              return (
                <Text
                  key={col.key}
                  style={{
                    width: col.width,
                    fontSize: 12,
                    color: colors.text.primary,
                  }}
                  numberOfLines={1}
                >
                  {value ?? "—"}
                </Text>
              );
            })}
          </View>
        )}
      />
    </View>
  );
}