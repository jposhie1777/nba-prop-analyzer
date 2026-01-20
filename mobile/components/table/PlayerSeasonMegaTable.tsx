import React, { useMemo, useState } from "react";
import {
  View,
  Text,
  ScrollView,
  Pressable,
  StyleSheet,
} from "react-native";
import { useTheme } from "@/store/useTheme";

type Row = Record<string, any>;

type Props = {
  rows: Row[];
};

export function PlayerSeasonMegaTable({ rows }: Props) {
  const { colors } = useTheme();

  /* ======================================
     FRONTEND DEDUPE (TEMPORARY)
  ====================================== */
  const deduped = useMemo(() => {
    const map = new Map<number, Row>();
    for (const r of rows) {
      if (!map.has(r.player_id)) {
        map.set(r.player_id, r);
      }
    }
    return Array.from(map.values());
  }, [rows]);

  /* ======================================
     SORT STATE
  ====================================== */
  const [sortKey, setSortKey] = useState<string>("last_name");
  const [direction, setDirection] = useState<"asc" | "desc">("asc");

  const sorted = useMemo(() => {
    return [...deduped].sort((a, b) => {
      const av = a[sortKey];
      const bv = b[sortKey];

      if (av == null) return 1;
      if (bv == null) return -1;

      if (typeof av === "number" && typeof bv === "number") {
        return direction === "asc" ? av - bv : bv - av;
      }

      return direction === "asc"
        ? String(av).localeCompare(String(bv))
        : String(bv).localeCompare(String(av));
    });
  }, [deduped, sortKey, direction]);

  const columns = Object.keys(deduped[0] ?? {}).filter(
    (c) => !["season_type"].includes(c)
  );

  function toggleSort(col: string) {
    if (col === sortKey) {
      setDirection((d) => (d === "asc" ? "desc" : "asc"));
    } else {
      setSortKey(col);
      setDirection("asc");
    }
  }

  return (
    <ScrollView horizontal showsHorizontalScrollIndicator>
      <View>
        {/* HEADER */}
        <View
          style={[
            styles.row,
            styles.header,
            {
              backgroundColor: colors.surface.card,
              borderBottomColor: colors.border.subtle,
            },
          ]}
        >
          {columns.map((col) => (
            <Pressable
              key={col}
              onPress={() => toggleSort(col)}
              style={styles.cell}
            >
              <Text
                style={[
                  styles.headerText,
                  { color: colors.text.muted },
                ]}
              >
                {col}
                {sortKey === col
                  ? direction === "asc"
                    ? " ▲"
                    : " ▼"
                  : ""}
              </Text>
            </Pressable>
          ))}
        </View>

        {/* ROWS */}
        {sorted.map((row, idx) => (
          <View
            key={`${row.player_id}-${idx}`}
            style={[
              styles.row,
              {
                backgroundColor:
                  idx % 2 === 0
                    ? colors.surface.card
                    : colors.surface.cardSoft,
              },
            ]}
          >
            {columns.map((col) => (
              <View key={col} style={styles.cell}>
                <Text
                  style={[
                    styles.cellText,
                    { color: colors.text.primary },
                  ]}
                  numberOfLines={1}
                >
                  {row[col] ?? "—"}
                </Text>
              </View>
            ))}
          </View>
        ))}
      </View>
    </ScrollView>
  );
}

const styles = StyleSheet.create({
  row: {
    flexDirection: "row",
  },

  header: {
    borderBottomWidth: StyleSheet.hairlineWidth,
  },

  cell: {
    width: 110,
    paddingVertical: 6,
    paddingHorizontal: 8,
  },

  headerText: {
    fontSize: 11,
    fontWeight: "700",
  },

  cellText: {
    fontSize: 11,
  },
});