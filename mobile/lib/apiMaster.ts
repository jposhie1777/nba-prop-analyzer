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
