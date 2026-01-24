// lib/apiMaster.ts
import { API_BASE } from "./config";

export async function fetchPlayerPropsMaster() {
  const url = `${API_BASE}/props/master`;
  console.log("📡 FETCH MASTER:", url);

  const res = await fetch(url, {
    credentials: "omit",
  });

  if (!res.ok) {
    throw new Error(`Master props error ${res.status}`);
  }

  return res.json();
}
