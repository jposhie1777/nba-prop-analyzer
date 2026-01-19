// utils/schemaToColumns.ts
import { ColumnSchema, ColumnConfig } from "@/types/schema";

const HIDDEN_COLUMNS = new Set([
  "team_id",
  "season",
  "season_type",
]);

export function schemaToColumns(
  schema: ColumnSchema[]
): ColumnConfig[] {
  return schema
    .filter(col => !HIDDEN_COLUMNS.has(col.name))
    .map(col => {
      const isPct = col.name.endsWith("_pct");
      const isRank = col.name.endsWith("_rank");

      return {
        key: col.name,
        label: col.name
          .replace("_pct", "%")
          .replace("_rank", " Rk")
          .toUpperCase(),
        width: isRank ? 60 : 70,
        isNumeric: col.type !== "STRING",
        formatter: isPct
          ? (v: number) => (v * 100).toFixed(1)
          : typeof v === "number"
          ? (v: number) => v.toFixed(1)
          : undefined,
      };
    });
}