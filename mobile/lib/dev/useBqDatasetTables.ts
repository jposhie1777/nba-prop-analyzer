// lib/dev/useBqDatasetTables.ts
import { useEffect, useState } from "react";
import Constants from "expo-constants";

const API_URL =
  Constants.expoConfig?.extra?.API_URL ??
  // @ts-ignore
  Constants.manifest?.extra?.API_URL;

export function useBqDatasetTables(dataset: string) {
  const [tables, setTables] = useState<string[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);

  async function load() {
    setLoading(true);
    try {
      const res = await fetch(
        `${API_URL}/dev/bq/datasets/${dataset}/tables`
      );
      const json = await res.json();
      setTables(json.tables ?? []);
    } catch (e: any) {
      setError(e.message);
    } finally {
      setLoading(false);
    }
  }

  useEffect(() => {
    load();
  }, [dataset]);

  return { tables, loading, error, reload: load };
}