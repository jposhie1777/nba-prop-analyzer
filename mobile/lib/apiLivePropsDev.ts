// lib/LivePorpsDev
import { API_BASE } from "./config";

export async function fetchLivePropsDev(limit = 100) {
  const res = await fetch(`${API_BASE}/live-props-dev?limit=${limit}`);

  if (!res.ok) {
    const text = await res.text();
    throw new Error(`Live props dev failed: ${text}`);
  }

  return res.json();
}
