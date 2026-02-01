// lib/dev/useBqDatasetTables.ts
import { useEffect, useState } from "react";
import Constants from "expo-constants";

const API_URL =
  Constants.expoConfig?.extra?.API_URL ??
  // @ts-ignore
  Constants.manifest?.extra?.API_URL;

type TableInfo = {
  name: string;
  type?: string;
  rowCount?: number | null;
};

function normalizeEntry(entry: any, fallbackType?: string): TableInfo | null {
  if (!entry) return null;
  if (typeof entry === "string") {
    return { name: entry, type: fallbackType };
  }
  if (typeof entry === "object") {
    const name = entry.name ?? entry.table_name ?? entry.table ?? "";
    if (!name) return null;
    return {
      name,
      type: entry.type ?? entry.table_type ?? fallbackType,
      rowCount:
        typeof entry.row_count === "number"
          ? entry.row_count
          : entry.rowCount ?? null,
    };
  }
  return null;
}

export function useBqDatasetTables(dataset: string) {
  const [items, setItems] = useState<TableInfo[]>([]);
  const [lastRefreshed, setLastRefreshed] = useState<string | null>(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);

  async function load() {
    setLoading(true);
    setError(null);

    try {
      const res = await fetch(
        `${API_URL}/dev/bq/datasets/${dataset}/tables`
      );
      const json = await res.json();
      const tables = Array.isArray(json.tables) ? json.tables : [];
      const views = Array.isArray(json.views) ? json.views : [];

      const normalized = [
        ...tables
          .map((entry: any) => normalizeEntry(entry, "BASE TABLE"))
          .filter(Boolean),
        ...views
          .map((entry: any) => normalizeEntry(entry, "VIEW"))
          .filter(Boolean),
      ] as TableInfo[];

      setItems(normalized);
      setLastRefreshed(json.last_refreshed ?? null);
    } catch (e: any) {
      setError(e.message ?? "Failed to load tables");
    } finally {
      setLoading(false);
    }
  }

  useEffect(() => {
    load();
  }, [dataset]);

  return {
    items,
    lastRefreshed,
    loading,
    error,
    reload: load,
  };
}
