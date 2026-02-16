import { useCallback, useEffect, useMemo, useRef, useState } from "react";

import { API_BASE } from "@/lib/config";

type QueryResult<T> = {
  data: T | null;
  loading: boolean;
  error: string | null;
  refetch: () => void;
};

type Params = Record<
  string,
  string | number | boolean | (string | number)[] | undefined
>;

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

const FETCH_TIMEOUT_MS = 30_000;

export function useEplQuery<T>(
  path: string,
  params?: Params,
  enabled: boolean = true
): QueryResult<T> {
  const [data, setData] = useState<T | null>(null);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const abortRef = useRef<AbortController | null>(null);

  const url = useMemo(() => {
    const qs = toQueryString(params);
    return `${API_BASE}${path}${qs}`;
  }, [path, params]);

  const fetchData = useCallback(async () => {
    if (!enabled) return;

    abortRef.current?.abort();
    const controller = new AbortController();
    abortRef.current = controller;
    const timer = setTimeout(() => controller.abort(), FETCH_TIMEOUT_MS);

    setLoading(true);
    setError(null);
    try {
      const res = await fetch(url, {
        credentials: "omit",
        signal: controller.signal,
      });
      if (!res.ok) {
        const text = await res.text();
        throw new Error(text || `HTTP ${res.status}`);
      }
      const json = await res.json();
      if (!controller.signal.aborted) {
        setData(json);
      }
    } catch (err: any) {
      if (err?.name === "AbortError") return;
      if (!controller.signal.aborted) {
        setError(err?.message ?? "Unknown error");
      }
    } finally {
      clearTimeout(timer);
      if (!controller.signal.aborted) {
        setLoading(false);
      }
    }
  }, [enabled, url]);

  useEffect(() => {
    fetchData();
    return () => {
      abortRef.current?.abort();
    };
  }, [fetchData]);

  return { data, loading, error, refetch: fetchData };
}
