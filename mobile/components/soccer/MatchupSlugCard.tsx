import { useMemo, useState } from "react";
import { Image, StyleSheet, Text, View } from "react-native";

import type { SoccerLeague, SoccerOddsSummary } from "@/hooks/soccer/useSoccerMatchups";

type Props = {
  league: SoccerLeague;
  homeTeam: string;
  awayTeam: string;
  startTimeUtc?: string | null;
  homeRecord?: string;
  awayRecord?: string;
  homeLogoUri?: string | null;
  awayLogoUri?: string | null;
  oddsSummary?: SoccerOddsSummary;
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

function formatOddsCell(value?: { odds_decimal?: number | null; odds_american?: number | null } | null): string {
  if (!value) return "—";
  const decimal = value.odds_decimal != null ? value.odds_decimal.toFixed(2) : null;
  const american =
    value.odds_american != null
      ? value.odds_american > 0
        ? `+${value.odds_american}`
        : `${value.odds_american}`
      : null;
  if (decimal && american) return `${decimal} (${american})`;
  if (decimal) return decimal;
  if (american) return american;
  return "—";
}

function TeamBadge({
  teamName,
  align,
  logoUri,
}: {
  teamName: string;
  align: "left" | "right";
  logoUri?: string | null;
}) {
  const [loadFailed, setLoadFailed] = useState(false);
  const uri = useMemo(() => logoUri ?? null, [logoUri]);

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
  homeTeam,
  awayTeam,
  startTimeUtc,
  homeRecord = "-",
  awayRecord = "-",
  homeLogoUri,
  awayLogoUri,
  oddsSummary,
}: Props) {
  const hasOdds = Boolean(oddsSummary?.home || oddsSummary?.draw || oddsSummary?.away);

  return (
    <View style={styles.root}>
      <View style={styles.topRow}>
        <TeamBadge teamName={homeTeam} align="left" logoUri={homeLogoUri} />
        <Text style={styles.recordText}>{homeRecord}</Text>
        <View style={styles.center}>
          <Text style={styles.dayText}>{formatDay(startTimeUtc)}</Text>
          <Text style={styles.timeText}>{formatTime(startTimeUtc)}</Text>
        </View>
        <Text style={styles.recordText}>{awayRecord}</Text>
        <TeamBadge teamName={awayTeam} align="right" logoUri={awayLogoUri} />
      </View>
      {hasOdds ? (
        <View style={styles.oddsWrap}>
          <View style={styles.oddsHeaderRow}>
            <Text style={styles.oddsHeader}>Home</Text>
            <Text style={styles.oddsHeader}>Draw</Text>
            <Text style={styles.oddsHeader}>Away</Text>
          </View>
          <View style={styles.oddsValueRow}>
            <Text style={styles.oddsValue}>{formatOddsCell(oddsSummary?.home)}</Text>
            <Text style={styles.oddsValue}>{formatOddsCell(oddsSummary?.draw)}</Text>
            <Text style={styles.oddsValue}>{formatOddsCell(oddsSummary?.away)}</Text>
          </View>
        </View>
      ) : null}
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
  oddsWrap: {
    marginTop: 10,
    borderTopWidth: StyleSheet.hairlineWidth,
    borderTopColor: "#1E293B",
    paddingTop: 8,
    gap: 4,
  },
  oddsHeaderRow: { flexDirection: "row", justifyContent: "space-between", gap: 8 },
  oddsHeader: {
    flex: 1,
    textAlign: "center",
    color: "#94A3B8",
    fontSize: 10,
    fontWeight: "700",
    textTransform: "uppercase",
    letterSpacing: 0.2,
  },
  oddsValueRow: { flexDirection: "row", justifyContent: "space-between", gap: 8 },
  oddsValue: { flex: 1, textAlign: "center", color: "#E2E8F0", fontSize: 12, fontWeight: "700" },
});
