import { useMemo, useState } from "react";
import { ActivityIndicator, Pressable, ScrollView, StyleSheet, Text, View } from "react-native";
import { useLocalSearchParams, useRouter } from "expo-router";

import {
  SoccerLeague,
  useSoccerMatchupDetail,
} from "@/hooks/soccer/useSoccerMatchups";
import { useTheme } from "@/store/useTheme";
import { useSoccerLeagueBadges } from "@/hooks/soccer/useSoccerLeagueBadges";
import { resolveBadgeForTeam } from "@/utils/soccerDisplay";
import { MatchupSlugCard } from "@/components/soccer/MatchupSlugCard";

type Props = {
  league: SoccerLeague;
  leagueTitle: string;
};

type SectionKey = "matchInfo" | "matchKeys" | "bettingStats" | "lastMatches";

type LastMatchRow = {
  side?: string | null;
  lm_date?: string | null;
  lm_ht?: string | null;
  lm_at?: string | null;
  lm_hscore?: number | null;
  lm_ascore?: number | null;
  lm_outcome?: string | null;
};

function stringifyValue(value: unknown): string {
  if (value == null) return "–";
  if (typeof value === "string") return value;
  if (typeof value === "number" || typeof value === "boolean") return String(value);
  try {
    return JSON.stringify(value);
  } catch {
    return String(value);
  }
}

function formatShortDate(value?: string | null) {
  if (!value) return "Date TBD";
  const date = new Date(value);
  if (Number.isNaN(date.getTime())) return "Date TBD";
  return date.toLocaleDateString(undefined, {
    month: "short",
    day: "numeric",
  });
}

function toTimestamp(value?: string | null) {
  if (!value) return 0;
  const time = new Date(value).getTime();
  return Number.isNaN(time) ? 0 : time;
}

function LastMatchLine({ row }: { row: LastMatchRow }) {
  return (
    <Text style={styles.valueText}>
      {formatShortDate(row.lm_date)} - {row.lm_ht ?? "Home"} {row.lm_hscore ?? "-"} - {row.lm_ascore ?? "-"}{" "}
      {row.lm_at ?? "Away"} ({row.lm_outcome ?? "N/A"})
    </Text>
  );
}

export function LeagueMatchupDetailScreen({ league, leagueTitle }: Props) {
  const router = useRouter();
  const { colors } = useTheme();
  const params = useLocalSearchParams<{
    matchId?: string;
    homeTeam?: string;
    awayTeam?: string;
    startTimeUtc?: string;
    homeRecord?: string;
    awayRecord?: string;
    homeLogoUri?: string;
    awayLogoUri?: string;
  }>();
  const matchId = Number(params.matchId);
  const { data, loading, error, refetch } = useSoccerMatchupDetail(league, Number.isFinite(matchId) ? matchId : null);
  const { data: badgeMap } = useSoccerLeagueBadges(league);

  const matchInfo = (data?.match_info ?? {}) as Record<string, unknown>;
  const homeTeam = params.homeTeam ?? (matchInfo.home_team as string | undefined) ?? "Home";
  const awayTeam = params.awayTeam ?? (matchInfo.away_team as string | undefined) ?? "Away";
  const startTimeUtc = params.startTimeUtc ?? (matchInfo.date_utc as string | undefined) ?? null;
  const homeRecentForm = (matchInfo.home_form as string | undefined) ?? "-";
  const awayRecentForm = (matchInfo.away_form as string | undefined) ?? "-";
  const homeRecord = params.homeRecord ?? homeRecentForm;
  const awayRecord = params.awayRecord ?? awayRecentForm;
  const homeLogoUri =
    params.homeLogoUri ??
    resolveBadgeForTeam(league, homeTeam, badgeMap);
  const awayLogoUri =
    params.awayLogoUri ??
    resolveBadgeForTeam(league, awayTeam, badgeMap);
  const [expanded, setExpanded] = useState<Record<SectionKey, boolean>>({
    matchInfo: false,
    matchKeys: false,
    bettingStats: false,
    lastMatches: false,
  });

  const matchInfoRows = Object.entries(matchInfo).filter(([key]) => !["match_id", "home_team", "away_team"].includes(key));
  const groupedLastMatches = useMemo(() => {
    const rows = (data?.last_matches ?? []) as LastMatchRow[];
    const home: LastMatchRow[] = [];
    const away: LastMatchRow[] = [];
    const other: LastMatchRow[] = [];

    rows.forEach((row) => {
      if (row.side === "home") {
        home.push(row);
      } else if (row.side === "away") {
        away.push(row);
      } else {
        other.push(row);
      }
    });

    const sortByDateDesc = (a: LastMatchRow, b: LastMatchRow) => toTimestamp(b.lm_date) - toTimestamp(a.lm_date);
    home.sort(sortByDateDesc);
    away.sort(sortByDateDesc);
    other.sort(sortByDateDesc);

    return { home, away, other };
  }, [data?.last_matches]);

  function toggleSection(section: SectionKey) {
    setExpanded((prev) => ({ ...prev, [section]: !prev[section] }));
  }

  function sectionChevron(section: SectionKey) {
    return expanded[section] ? "▾" : "▸";
  }

  return (
    <ScrollView style={styles.screen} contentContainerStyle={styles.content}>
      <View style={styles.actionRow}>
        <Pressable onPress={() => router.back()} style={[styles.actionButton, { borderColor: colors.border.subtle }]}>
          <Text style={styles.actionText}>← Back</Text>
        </Pressable>
        <Pressable onPress={() => router.push("/(tabs)/home")} style={[styles.actionButton, { borderColor: colors.border.subtle }]}>
          <Text style={styles.actionText}>⌂ Home</Text>
        </Pressable>
      </View>

      <View style={[styles.panel, { borderColor: colors.border.subtle }]}>
        <Text style={styles.eyebrow}>{leagueTitle}</Text>
        <MatchupSlugCard
          league={league}
          homeTeam={homeTeam}
          awayTeam={awayTeam}
          startTimeUtc={startTimeUtc}
          homeRecord={homeRecord}
          awayRecord={awayRecord}
          homeLogoUri={homeLogoUri}
          awayLogoUri={awayLogoUri}
        />
      </View>

      {loading ? <ActivityIndicator color="#A78BFA" /> : null}

      {error ? (
        <Pressable onPress={refetch} style={[styles.panel, { borderColor: colors.border.subtle }]}>
          <Text style={styles.errorTitle}>Failed to load matchup detail.</Text>
          <Text style={styles.errorText}>{error}</Text>
          <Text style={styles.errorRetry}>Tap to retry</Text>
        </Pressable>
      ) : null}

      {data ? (
        <>
          <View style={[styles.panel, { borderColor: colors.border.subtle }]}>
            <Pressable onPress={() => toggleSection("matchInfo")} style={styles.sectionHeader}>
              <Text style={styles.sectionTitle}>Match Info</Text>
              <Text style={styles.sectionToggle}>{sectionChevron("matchInfo")}</Text>
            </Pressable>
            {expanded.matchInfo ? (
              matchInfoRows.length ? (
                matchInfoRows.map(([key, value]) => (
                  <View key={key} style={styles.row}>
                    <Text style={styles.keyText}>{key}</Text>
                    <Text style={styles.valueText}>{stringifyValue(value)}</Text>
                  </View>
                ))
              ) : (
                <Text style={styles.emptyText}>No match info available.</Text>
              )
            ) : null}
          </View>

          <View style={[styles.panel, { borderColor: colors.border.subtle }]}>
            <Pressable onPress={() => toggleSection("matchKeys")} style={styles.sectionHeader}>
              <Text style={styles.sectionTitle}>Match Keys - Betting Insights</Text>
              <Text style={styles.sectionToggle}>{sectionChevron("matchKeys")}</Text>
            </Pressable>
            {expanded.matchKeys ? (
              data.match_keys.length ? (
                data.match_keys.map((row, idx) => (
                  <Text key={`mk-${idx}`} style={styles.valueText}>
                    #{row.rank ?? idx + 1} {row.statement ?? "No statement"}
                  </Text>
                ))
              ) : (
                <Text style={styles.emptyText}>No match keys available.</Text>
              )
            ) : null}
          </View>

          <View style={[styles.panel, { borderColor: colors.border.subtle }]}>
            <Pressable onPress={() => toggleSection("bettingStats")} style={styles.sectionHeader}>
              <Text style={styles.sectionTitle}>Betting Stats</Text>
              <Text style={styles.sectionToggle}>{sectionChevron("bettingStats")}</Text>
            </Pressable>
            {expanded.bettingStats ? (
              data.betting_stats.length ? (
                data.betting_stats.map((row, idx) => (
                  <Text key={`bs-${idx}`} style={styles.valueText}>
                    {row.category ?? "category"} • {row.sub_tab ?? "sub_tab"} • {row.label ?? "label"}: {row.value ?? "–"}
                  </Text>
                ))
              ) : (
                <Text style={styles.emptyText}>No betting stats available.</Text>
              )
            ) : null}
          </View>

          <View style={[styles.panel, { borderColor: colors.border.subtle }]}>
            <Pressable onPress={() => toggleSection("lastMatches")} style={styles.sectionHeader}>
              <Text style={styles.sectionTitle}>Last Matches</Text>
              <Text style={styles.sectionToggle}>{sectionChevron("lastMatches")}</Text>
            </Pressable>
            {expanded.lastMatches ? (
              data.last_matches.length ? (
                <>
                  <Text style={styles.groupTitle}>{homeTeam}</Text>
                  {groupedLastMatches.home.length ? (
                    groupedLastMatches.home.map((row, idx) => <LastMatchLine key={`home-lm-${idx}`} row={row} />)
                  ) : (
                    <Text style={styles.emptyText}>No recent matches found.</Text>
                  )}

                  <Text style={styles.groupTitle}>{awayTeam}</Text>
                  {groupedLastMatches.away.length ? (
                    groupedLastMatches.away.map((row, idx) => <LastMatchLine key={`away-lm-${idx}`} row={row} />)
                  ) : (
                    <Text style={styles.emptyText}>No recent matches found.</Text>
                  )}

                  {groupedLastMatches.other.length ? (
                    <>
                      <Text style={styles.groupTitle}>Other</Text>
                      {groupedLastMatches.other.map((row, idx) => <LastMatchLine key={`other-lm-${idx}`} row={row} />)}
                    </>
                  ) : null}
                </>
              ) : (
                <Text style={styles.emptyText}>No last matches available.</Text>
              )
            ) : null}
          </View>
        </>
      ) : null}
    </ScrollView>
  );
}

const styles = StyleSheet.create({
  screen: { flex: 1, backgroundColor: "#050A18" },
  content: { padding: 16, gap: 10, paddingBottom: 40 },
  actionRow: { flexDirection: "row", gap: 8 },
  actionButton: {
    borderWidth: StyleSheet.hairlineWidth,
    borderRadius: 12,
    backgroundColor: "#0B1529",
    paddingHorizontal: 12,
    paddingVertical: 8,
  },
  actionText: { color: "#E9F2FF", fontWeight: "700", fontSize: 12 },
  panel: { borderWidth: StyleSheet.hairlineWidth, borderRadius: 14, backgroundColor: "#0B1529", padding: 12, gap: 8 },
  eyebrow: { color: "#90B3E9", fontSize: 11, fontWeight: "700" },
  sectionTitle: { color: "#93C5FD", fontSize: 14, fontWeight: "700" },
  sectionHeader: { flexDirection: "row", alignItems: "center", justifyContent: "space-between" },
  sectionToggle: { color: "#C4B5FD", fontWeight: "800", fontSize: 14 },
  groupTitle: { color: "#A7C0E8", fontSize: 12, fontWeight: "800", marginTop: 6 },
  row: { gap: 2 },
  keyText: { color: "#C4B5FD", fontSize: 11, fontWeight: "700" },
  valueText: { color: "#E5E7EB", fontSize: 12, lineHeight: 18 },
  emptyText: { color: "#94A3B8", fontSize: 12 },
  errorTitle: { color: "#FCA5A5", fontWeight: "700" },
  errorText: { color: "#FECACA", fontSize: 12 },
  errorRetry: { color: "#E5E7EB", marginTop: 4, fontSize: 12 },
});
