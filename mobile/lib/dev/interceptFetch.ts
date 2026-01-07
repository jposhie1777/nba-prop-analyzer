// /lib/dev/interceptFetch.ts
import { useDevStore } from "@/lib/dev/devStore";

let installed = false;

export function installFetchInterceptor() {
  if (installed || !__DEV__) return;
  installed = true;

  const originalFetch = global.fetch;

  global.fetch = async (input: RequestInfo, init?: RequestInit) => {
    const start = Date.now();

    const method =
      (init?.method ?? (input instanceof Request ? input.method : "GET")).toUpperCase();

    const url =
      typeof input === "string"
        ? input
        : input instanceof Request
        ? input.url
        : String(input);

    try {
      const res = await originalFetch(input as any, init);
      const ms = Date.now() - start;

      useDevStore.getState().actions.logNetwork({
        method,
        url,
        status: res.status,
        ms,
      });

      return res;
    } catch (err: any) {
      const ms = Date.now() - start;

      useDevStore.getState().actions.logNetwork({
        method,
        url,
        ms,
        error: err?.message ?? String(err),
      });

      throw err;
    }
  };
}