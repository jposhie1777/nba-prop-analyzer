import { API_BASE } from "./config";

export async function fetchLiveProps({
  limit,
  cursor,
}: {
  limit: number;
  cursor?: string;
}) {
  const params = new URLSearchParams({
    limit: String(limit),
  });

  if (cursor) {
    params.append("cursor", cursor);
  }

  const res = await fetch(
    `${API_BASE}/live-props?${params.toString()}`,
    { credentials: "omit" }
  );

  if (!res.ok) {
    throw new Error("Failed to fetch live props");
  }

  return res.json();
}