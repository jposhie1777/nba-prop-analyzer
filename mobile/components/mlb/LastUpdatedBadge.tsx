import { StyleSheet, Text } from "react-native";

function formatRefreshed(iso?: string | null): string | null {
  if (!iso) return null;
  const d = new Date(iso);
  if (Number.isNaN(d.getTime())) return null;
  return (
    d.toLocaleString("en-US", {
      timeZone: "America/New_York",
      month: "short",
      day: "numeric",
      hour: "numeric",
      minute: "2-digit",
      hour12: true,
    }) + " ET"
  );
}

export function LastUpdatedBadge({
  refreshedAt,
  cacheSource,
}: {
  refreshedAt?: string | null;
  cacheSource?: string | null;
}) {
  const pretty = formatRefreshed(refreshedAt);
  if (!pretty) return null;
  const suffix = cacheSource === "neon" ? " · cache" : cacheSource ? ` · ${cacheSource}` : "";
  return <Text style={styles.text}>Updated {pretty}{suffix}</Text>;
}

const styles = StyleSheet.create({
  text: {
    color: "#6B7FA0",
    fontSize: 11,
    fontStyle: "italic",
    marginTop: 6,
  },
});
