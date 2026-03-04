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

function buildUrl(base: string, path: string, params?: Params): string {
  const normalizedBase = base.replace(/\/+$/, "");
  const normalizedPath = path.startsWith("/") ? path : `/${path}`;
  const qs = toQueryString(params);
  return `${normalizedBase}${normalizedPath}${qs}`;
}

function networkFetchFailed(err: unknown): boolean {
  const message = err instanceof Error ? err.message : String(err ?? "");
  return message.toLowerCase().includes("failed to fetch");
}

function getFallbackBases(base: string): string[] {
  const candidates = [base];

  const pulseHostVariant = base.replace(
    /https:\/\/mobile-api-(\d+)\.us-central1\.run\.app/i,
    "https://pulse-mobile-api-$1.us-central1.run.app"
  );
  const mobileHostVariant = base.replace(
    /https:\/\/pulse-mobile-api-(\d+)\.us-central1\.run\.app/i,
    "https://mobile-api-$1.us-central1.run.app"
  );

  candidates.push(pulseHostVariant, mobileHostVariant);

  if (typeof window !== "undefined") {
    const origin = window.location?.origin;
    if (origin) {
      candidates.push(`${origin}/api`);
      candidates.push(origin);
    }
    candidates.push("/api");
  }

  return Array.from(new Set(candidates));
}

export function useEplQuery<T>(
  path: string,
  params?: Params,
  enabled: boolean = true
): QueryResult<T> {
  const [data, setData] = useState<T | null>(null);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const abortRef = useRef<AbortController | null>(null);
  const mountedRef = useRef(true);

  const urls = useMemo(
    () => getFallbackBases(API_BASE).map((base) => buildUrl(base, path, params)),
    [path, params]
  );

  const fetchData = useCallback(async () => {
    if (!enabled) return;

    abortRef.current?.abort();
    const controller = new AbortController();
    abortRef.current = controller;
    let timedOut = false;
    const timer = setTimeout(() => {
      timedOut = true;
      controller.abort();
    }, FETCH_TIMEOUT_MS);

    if (mountedRef.current) {
      setLoading(true);
      setError(null);
    }
    try {
      let lastNetworkError: unknown = null;

      for (const url of urls) {
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
          if (!controller.signal.aborted && mountedRef.current) {
            setData(json);
          }
          return;
        } catch (err) {
          if (controller.signal.aborted) {
            throw err;
          }

          if (networkFetchFailed(err)) {
            lastNetworkError = err;
            continue;
          }

          throw err;
        }
      }

      if (lastNetworkError) {
        throw new Error(`Failed to fetch (tried: ${urls.join(", ")})`);
      }
    } catch (err: any) {
      if (!mountedRef.current) return;
      if (err?.name === "AbortError") {
        if (timedOut) {
          setError("Request timed out. Please retry.");
        }
        return;
      }
      if (!controller.signal.aborted) {
        setError(err?.message ?? "Unknown error");
      }
    } finally {
      clearTimeout(timer);
      if (mountedRef.current) {
        setLoading(false);
      }
    }
  }, [enabled, urls]);

  useEffect(() => {
    mountedRef.current = true;
    fetchData();
    return () => {
      mountedRef.current = false;
      abortRef.current?.abort();
    };
  }, [fetchData]);

  return { data, loading, error, refetch: fetchData };
}
