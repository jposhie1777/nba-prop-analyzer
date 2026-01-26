import { API_BASE } from "./config";

export async function fetchLiveProps(limit = 100) {
  const url = `${API_BASE}/live-props?limit=${limit}`;
  console.log("📡 [LIVE PROPS]", url);

  const res = await fetch(url, { credentials: "omit" });

  if (!res.ok) {
    const text = await res.text();
    throw new Error(`Live props failed ${res.status}: ${text}`);
  }

  const json = await res.json();
  return json.props ?? [];
}