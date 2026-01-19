// utils/schemaToColumns.ts
import { ColumnSchema, ColumnConfig } from "@/types/schema";

const HIDDEN_COLUMNS = new Set([
  "team_id",
  "season",
  "season_type",
]);

export function schemaToColumns(schema: ColumnSchema[]) {
  return schema
    .filter(col => !["team_id", "season", "season_type"].includes(col.name))
    .map(col => {
      const isName =
        col.name === "team_name" || col.name === "team_abbr";
      const isRank = col.name.endsWith("_rank");
      const isPct = col.name.endsWith("_pct");

      return {
        key: col.name,
        label: col.name
          .replace("_pct", "%")
          .replace("_rank", " Rk")
          .toUpperCase(),

        // 🔑 smarter widths
        width: isName
          ? 160
          : isRank
          ? 60
          : 80,

        formatter: isPct
          ? (v: number) => (v * 100).toFixed(1)
          : undefined,
      };
    });
}
}