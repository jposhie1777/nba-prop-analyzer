// components/table/AutoSortableTable.tsx
import {
  View,
  Text,
  FlatList,
  Pressable,
  ScrollView,
} from "react-native";
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
    <View
      style={{
        flex: 1,
        backgroundColor: colors.surface.card,
        borderRadius: 12,
        borderWidth: 1,
        borderColor: colors.border.subtle,
        overflow: "hidden",
      }}
    >
      {/* ======================================================
          HORIZONTAL SCROLL WRAPPER
      ====================================================== */}
      <ScrollView
        horizontal
        showsHorizontalScrollIndicator={false}
        contentContainerStyle={{ minWidth: "100%" }}
      >
        <View>
          {/* ======================================================
              HEADER
          ====================================================== */}
          <View
            style={{
              flexDirection: "row",
              paddingVertical: 10,
              paddingHorizontal: 8,
              backgroundColor: colors.surface.cardSoft,
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
                      fontSize: 12,
                      fontWeight: "700",
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

          {/* ======================================================
              ROWS (VERTICAL SCROLL)
          ====================================================== */}
          <FlatList
            data={sortedData}
            keyExtractor={(_, i) => String(i)}
            showsVerticalScrollIndicator={false}
            contentContainerStyle={{ paddingBottom: 12 }}
            renderItem={({ item, index }) => {
              const isEven = index % 2 === 0;

              return (
                <View
                  style={{
                    flexDirection: "row",
                    paddingVertical: 8,
                    paddingHorizontal: 8,
                    backgroundColor: isEven
                      ? colors.surface.card
                      : colors.surface.cardSoft,
                    borderBottomWidth: 1,
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
              );
            }}
          />
        </View>
      </ScrollView>
    </View>
  );
}