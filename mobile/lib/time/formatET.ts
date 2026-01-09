export function formatET(iso: string | null) {
  if (!iso) return "TBD";

  return new Date(iso).toLocaleTimeString("en-US", {
    hour: "numeric",
    minute: "2-digit",
    timeZone: "America/New_York",
  });
}
