import { useCallback, useEffect, useMemo, useState } from "react";

import { API_BASE } from "@/lib/config";

const API = API_BASE;

type QueryResult<T> = {
  data: T | null;
  loading: boolean;
  error: string | null;
  refetch: () => void;
};

type Params = Record<string, string | number | boolean | Array<string | number> | undefined>;

function toQueryString(params?: Params): string {
  if (!params) return "";
  const search = new URLSearchParams();
  Object.entries(params).forEach(([key, value]) => {
    if (value === undefined || value === null) return;
    if (Array.isArray(value)) {
      value.forEach((entry) => search.append(key, String(entry)));
    } else {
      search.append(key, String(value));
    }
  });
  const qs = search.toString();
  return qs ? `?${qs}` : "";
}

export function usePgaQuery<T>(
  path: string,
  params?: Params,
  enabled: boolean = true
): QueryResult<T> {
  const [data, setData] = useState<T | null>(null);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);

  const url = useMemo(() => {
    const qs = toQueryString(params);
    return API ? `${API}${path}${qs}` : "";
  }, [path, params]);

  const fetchData = useCallback(async () => {
    if (!enabled) return;
    setLoading(true);
    setError(null);
    try {
      if (!API) {
        throw new Error("API URL not configured");
      }
      const res = await fetch(url, { credentials: "omit" });
      if (!res.ok) {
        const text = await res.text();
        throw new Error(text || `HTTP ${res.status}`);
      }
      const json = await res.json();
      setData(json);
    } catch (err: any) {
      const message = err?.message ?? "Unknown error";
      if (message === "Failed to fetch") {
        setError("Network error. Check API URL or CORS settings.");
      } else {
        setError(message);
      }
    } finally {
      setLoading(false);
    }
  }, [enabled, url]);

  useEffect(() => {
    fetchData();
  }, [fetchData]);

  return { data, loading, error, refetch: fetchData };
}
