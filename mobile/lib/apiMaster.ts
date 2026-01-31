// mobile/lib/apiMaster.ts
import { API_BASE } from "./config";

/* 🔑 RE-EXPORT so other modules can import it */
export { API_BASE };

type FetchArgs = {
  limit?: number;
  offset?: number;
};

export async function fetchPlayerPropsMaster(
  { limit, offset }: FetchArgs = {}
) {
  const params = new URLSearchParams();

  if (limit != null) params.set("limit", String(limit));
  if (offset != null) params.set("offset", String(offset));

  const url =
    params.toString().length > 0
      ? `${API_BASE}/props?${params.toString()}`
      : `${API_BASE}/props`;

  console.log("📡 [MASTER FETCH]", url);

  const res = await fetch(url, { credentials: "omit" });

  if (!res.ok) {
    const text = await res.text();
    console.error("❌ [MASTER FETCH FAILED]", res.status, text);
    throw new Error(`Master props error ${res.status}`);
  }

  const json = await res.json();

  return json.props ?? [];
}

export async function fetchPlayerPositions() {
  const url = `${API_BASE}/players/positions`;

  console.log("📡 [PLAYER POSITIONS FETCH]", url);

  const res = await fetch(url, { credentials: "omit" });

  if (!res.ok) {
    const text = await res.text();
    console.error(
      "❌ [PLAYER POSITIONS FETCH FAILED]",
      res.status,
      text
    );
    throw new Error(`Player positions error ${res.status}`);
  }

  const json = await res.json();

  return json.rows ?? [];
}

/* ======================================================
   BAD LINES
====================================================== */
export async function fetchBadLines({
  min_score = 1.25,
  limit = 50,
}: {
  min_score?: number;
  limit?: number;
} = {}) {
  const params = new URLSearchParams();
  params.set("min_score", String(min_score));
  params.set("limit", String(limit));

  const url = `${API_BASE}/bad-lines?${params.toString()}`;

  console.log("📡 [BAD LINES FETCH]", url);

  const res = await fetch(url, { credentials: "omit" });

  if (!res.ok) {
    const text = await res.text();
    console.error("❌ [BAD LINES FETCH FAILED]", res.status, text);
    throw new Error(`Bad lines fetch failed ${res.status}`);
  }

  const json = await res.json();

  console.log(
    "📦 [BAD LINES FETCH] count:",
    json?.bad_lines?.length
  );

  return json;
}

/* ======================================================
   LIVE BAD LINES (during active games)
====================================================== */
export async function fetchLiveBadLines({
  min_edge = 0.15,
  limit = 50,
}: {
  min_edge?: number;
  limit?: number;
} = {}) {
  const params = new URLSearchParams();
  params.set("min_edge", String(min_edge));
  params.set("limit", String(limit));

  const url = `${API_BASE}/bad-lines/live?${params.toString()}`;

  console.log("📡 [LIVE BAD LINES FETCH]", url);

  const res = await fetch(url, { credentials: "omit" });

  if (!res.ok) {
    const text = await res.text();
    console.error("❌ [LIVE BAD LINES FETCH FAILED]", res.status, text);
    throw new Error(`Live bad lines fetch failed ${res.status}`);
  }

  const json = await res.json();

  console.log(
    "📦 [LIVE BAD LINES FETCH] count:",
    json?.bad_lines?.length
  );

  return json;
}
