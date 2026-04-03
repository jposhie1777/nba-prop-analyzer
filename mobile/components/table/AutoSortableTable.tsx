// components/table/AutoSortableTable.tsx
import { useMemo } from "react";
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

const CHAR_W = 7.5; // avg px per char at fontSize 12
const PAD = 24;      // paddingRight + buffer
const MIN_COL = 44;
const MAX_COL = 240;
const CHECK_W = 36;

export function AutoSortableTable<T>({
  data,
  columns,
  defaultSort,
  onRowPress,
  autoWidth,
  selectable,
  selectedKeys,
  onToggle,
  rowKey,
}: {
  data: T[];
  columns: ColumnConfig[];
  defaultSort: keyof T;
  onRowPress?: (row: T) => void;
  /** Compute column widths from data content (enables horizontal scroll) */
  autoWidth?: boolean;
  /** Show selection checkboxes */
  selectable?: boolean;
  /** Set of selected row keys */
  selectedKeys?: Set<string>;
  /** Called when a row checkbox is toggled */
  onToggle?: (key: string, row: T) => void;
  /** Extract unique key from row (required when selectable) */
  rowKey?: (row: T) => string;
}) {
  const { colors } = useTheme();

  const {
    sortedData,
    sortKey,
    direction,
    toggleSort,
  } = useSortableData(data, defaultSort);

  // ── Auto-width calculation ──────────────────────────────────────────────
  const colWidths = useMemo(() => {
    if (!autoWidth) return columns.map((c) => c.width);

    return columns.map((col) => {
      const headerLen = (col.label.length + 2) * CHAR_W + PAD;
      let maxData = 0;
      for (const row of data) {
        const raw = (row as any)[col.key];
        const val = col.formatter ? col.formatter(raw) : String(raw ?? "\u2014");
        maxData = Math.max(maxData, val.length * CHAR_W + PAD);
      }
      const optimal = Math.max(headerLen, maxData);
      // Use col.width as minimum floor when autoWidth is on
      const floor = Math.max(MIN_COL, col.width ?? MIN_COL);
      return Math.min(MAX_COL, Math.max(floor, Math.ceil(optimal)));
    });
  }, [autoWidth, data, columns]);

  const totalWidth = (selectable ? CHECK_W : 0) + colWidths.reduce((a, b) => a + b, 0);

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
      <ScrollView
        horizontal
        showsHorizontalScrollIndicator={true}
        persistentScrollbar={true}
      >
        <View style={{ minWidth: Math.max(totalWidth, 320) }}>
          {/* ── HEADER ── */}
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
            {selectable && (
              <View style={{ width: CHECK_W, alignItems: "center", justifyContent: "center" }} />
            )}
            {columns.map((col, ci) => {
              const isActive = sortKey === col.key;
              return (
                <Pressable
                  key={col.key}
                  onPress={() => toggleSort(col.key as keyof T)}
                  style={{ width: colWidths[ci], paddingRight: 4 }}
                >
                  <Text
                    numberOfLines={1}
                    style={{
                      fontSize: 12,
                      fontWeight: "700",
                      color: isActive ? colors.accent.primary : colors.text.secondary,
                    }}
                  >
                    {col.label}
                    {isActive ? (direction === "asc" ? " \u25B2" : " \u25BC") : ""}
                  </Text>
                </Pressable>
              );
            })}
          </View>

          {/* ── ROWS ── */}
          <FlatList
            data={sortedData}
            keyExtractor={(item, i) => (rowKey ? rowKey(item) : String(i))}
            showsVerticalScrollIndicator={false}
            contentContainerStyle={{ paddingBottom: 12 }}
            renderItem={({ item, index }) => {
              const isEven = index % 2 === 0;
              const key = rowKey ? rowKey(item) : "";
              const isSelected = selectable && selectedKeys?.has(key);

              const rowContent = (
                <View
                  style={{
                    flexDirection: "row",
                    paddingVertical: 8,
                    paddingHorizontal: 8,
                    backgroundColor: isSelected
                      ? "rgba(74,222,128,0.10)"
                      : isEven
                      ? colors.surface.card
                      : colors.surface.cardSoft,
                    borderBottomWidth: 1,
                    borderColor: colors.border.subtle,
                  }}
                >
                  {selectable && (
                    <Pressable
                      onPress={() => onToggle?.(key, item)}
                      style={{
                        width: CHECK_W,
                        alignItems: "center",
                        justifyContent: "center",
                      }}
                      hitSlop={8}
                    >
                      <Text style={{ fontSize: 16, color: isSelected ? "#4ADE80" : colors.text.muted }}>
                        {isSelected ? "\u2611" : "\u2610"}
                      </Text>
                    </Pressable>
                  )}
                  {columns.map((col, ci) => {
                    const raw = (item as any)[col.key];
                    const value = col.formatter ? col.formatter(raw) : raw;
                    return (
                      <Text
                        key={col.key}
                        numberOfLines={1}
                        style={{
                          width: colWidths[ci],
                          paddingRight: 4,
                          fontSize: 12,
                          color: colors.text.primary,
                        }}
                      >
                        {value ?? "\u2014"}
                      </Text>
                    );
                  })}
                </View>
              );

              if (onRowPress) {
                return (
                  <Pressable onPress={() => onRowPress(item)}>
                    {rowContent}
                  </Pressable>
                );
              }
              return rowContent;
            }}
          />
        </View>
      </ScrollView>
    </View>
  );
}
