import { useMemo, useState } from "react";
import { Image, StyleSheet, Text, View } from "react-native";

import type { SoccerLeague } from "@/hooks/soccer/useSoccerMatchups";
import { getSoccerTeamLogoUrl } from "@/utils/soccerDisplay";

type Props = {
  league: SoccerLeague;
  homeTeam: string;
  awayTeam: string;
  startTimeUtc?: string | null;
  homeRecord?: string;
  awayRecord?: string;
};

function formatDay(value?: string | null) {
  if (!value) return "TBD";
  const date = new Date(value);
  if (Number.isNaN(date.getTime())) return "TBD";
  return date.toLocaleDateString(undefined, {
    weekday: "short",
    month: "short",
    day: "numeric",
  });
}

function formatTime(value?: string | null) {
  if (!value) return "TBD";
  const date = new Date(value);
  if (Number.isNaN(date.getTime())) return "TBD";
  return date.toLocaleTimeString(undefined, {
    hour: "numeric",
    minute: "2-digit",
  });
}

function initials(value: string): string {
  const parts = value.split(" ").filter(Boolean);
  if (!parts.length) return "?";
  if (parts.length === 1) return parts[0].slice(0, 2).toUpperCase();
  return `${parts[0][0] ?? ""}${parts[parts.length - 1][0] ?? ""}`.toUpperCase();
}

function TeamBadge({
  league,
  teamName,
  align,
}: {
  league: SoccerLeague;
  teamName: string;
  align: "left" | "right";
}) {
  const [loadFailed, setLoadFailed] = useState(false);
  const uri = useMemo(() => getSoccerTeamLogoUrl(league, teamName), [league, teamName]);

  return (
    <View style={[styles.teamBadge, align === "right" ? { alignItems: "flex-end" } : null]}>
      {uri && !loadFailed ? (
        <Image
          source={{ uri }}
          style={styles.logo}
          onError={() => setLoadFailed(true)}
        />
      ) : (
        <View style={styles.fallbackLogo}>
          <Text style={styles.fallbackText}>{initials(teamName)}</Text>
        </View>
      )}
      <Text style={[styles.teamName, align === "right" ? styles.teamNameRight : null]} numberOfLines={1}>
        {teamName}
      </Text>
    </View>
  );
}

export function MatchupSlugCard({
  league,
  homeTeam,
  awayTeam,
  startTimeUtc,
  homeRecord = "-",
  awayRecord = "-",
}: Props) {
  return (
    <View style={styles.root}>
      <View style={styles.topRow}>
        <TeamBadge league={league} teamName={homeTeam} align="left" />
        <Text style={styles.recordText}>{homeRecord}</Text>
        <View style={styles.center}>
          <Text style={styles.dayText}>{formatDay(startTimeUtc)}</Text>
          <Text style={styles.timeText}>{formatTime(startTimeUtc)}</Text>
        </View>
        <Text style={styles.recordText}>{awayRecord}</Text>
        <TeamBadge league={league} teamName={awayTeam} align="right" />
      </View>
    </View>
  );
}

const styles = StyleSheet.create({
  root: { width: "100%" },
  topRow: { flexDirection: "row", alignItems: "center", justifyContent: "space-between", gap: 8 },
  teamBadge: { flex: 1.55, gap: 6 },
  logo: { width: 28, height: 28, borderRadius: 14, backgroundColor: "#111827" },
  fallbackLogo: {
    width: 28,
    height: 28,
    borderRadius: 14,
    backgroundColor: "#1F2937",
    alignItems: "center",
    justifyContent: "center",
  },
  fallbackText: { color: "#E5E7EB", fontSize: 10, fontWeight: "800" },
  teamName: { color: "#E5E7EB", fontSize: 13, fontWeight: "700" },
  teamNameRight: { textAlign: "right" },
  recordText: {
    flex: 0.8,
    color: "#B7C5DD",
    fontSize: 14,
    fontWeight: "700",
    textAlign: "center",
  },
  center: { flex: 1.4, alignItems: "center", gap: 2 },
  dayText: { color: "#B7C5DD", fontSize: 12, fontWeight: "600" },
  timeText: { color: "#F8FAFC", fontSize: 18, fontWeight: "800" },
});
