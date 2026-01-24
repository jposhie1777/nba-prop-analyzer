// lib/apiMaster.ts
import { API_BASE } from "./config";

export async function fetchPlayerPropsMaster() {
  const url = `${API_BASE}/props`;
  console.log("📡 FETCH MASTER:", url);

  const res = await fetch(url, { credentials: "omit" });

  if (!res.ok) {
    throw new Error(`Master props error ${res.status}`);
  }

  const json = await res.json();
  return json.props; // ✅ IMPORTANT
}
