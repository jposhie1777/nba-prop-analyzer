// lib/apiMaster.ts
import { API_BASE } from "./config";

export async function fetchPlayerPropsMaster() {
  const url = `${API_BASE}/props`;
  console.log("📡 [MASTER FETCH]", url);

  const res = await fetch(url, { credentials: "omit" });

  if (!res.ok) {
    const text = await res.text();
    console.error("❌ [MASTER FETCH FAILED]", res.status, text);
    throw new Error(`Master props error ${res.status}`);
  }

  const json = await res.json();

  console.log("📦 [MASTER FETCH] count:", json?.props?.length);
  console.log("🧪 [MASTER FETCH] sample:", json?.props?.[0]);

  return json.props ?? [];
}
