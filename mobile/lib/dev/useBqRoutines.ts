import { useEffect, useState } from "react";
import Constants from "expo-constants";

const API_URL =
  Constants.expoConfig?.extra?.API_URL ??
  // @ts-ignore
  Constants.manifest?.extra?.API_URL;

export type RoutineInfo = {
  name: string;
  type?: string;
  definition?: string | null;
};

export function useBqRoutines(dataset: string) {
  const [items, setItems] = useState<RoutineInfo[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);

  async function load() {
    setLoading(true);
    setError(null);

    try {
      const res = await fetch(
        `${API_URL}/dev/bq/datasets/${dataset}/routines`
      );
      const json = await res.json();
      setItems(json.routines ?? []);
    } catch (e: any) {
      setError(e.message ?? "Failed to load routines");
    } finally {
      setLoading(false);
    }
  }

  useEffect(() => {
    load();
  }, [dataset]);

  return {
    items,
    loading,
    error,
    reload: load,
  };
}
