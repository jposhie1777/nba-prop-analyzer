// useBqTablePreview
import { useState } from "react";
import Constants from "expo-constants";

const API_URL =
  Constants.expoConfig?.extra?.API_URL ??
  // @ts-ignore
  Constants.manifest?.extra?.API_URL;

type Column = {
  column_name: string;
  data_type: string;
  is_nullable: string;
};

export function useBqTablePreview() {
  const [loading, setLoading] = useState(false);
  const [columns, setColumns] = useState<Column[] | null>(null);
  const [row, setRow] = useState<any | null>(null);
  const [error, setError] = useState<string | null>(null);

  async function load(dataset: string, table: string) {
    setLoading(true);
    setError(null);

    try {
      const res = await fetch(
        `${API_URL}/dev/bq/table-preview?dataset=${dataset}&table=${table}`
      );

      if (!res.ok) {
        throw new Error(`HTTP ${res.status}`);
      }

      const json = await res.json();
      setColumns(json.columns ?? []);
      setRow(json.example_row ?? null);
    } catch (e: any) {
      setError(e.message ?? "Failed to load table preview");
    } finally {
      setLoading(false);
    }
  }

  return {
    loading,
    columns,
    row,
    error,
    load,
  };
}