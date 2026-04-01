import { ActivityIndicator, Image, Pressable, ScrollView, StyleSheet, Text, View } from "react-native";
import { useRouter } from "expo-router";

import { useMlbUpcomingDebug, useMlbUpcomingGames } from "@/hooks/mlb/useMlbMatchups";
import { useTheme } from "@/store/useTheme";
import { formatET } from "@/lib/time/formatET";
import { getMlbTeamLogo } from "@/utils/mlbLogos";

function scoreColor(grade?: string | null) {
  const normalized = (grade || "").toUpperCase();
  if (normalized === "IDEAL") return "#22C55E";
  if (normalized === "FAVORABLE") return "#10B981";
  if (normalized === "AVERAGE") return "#F59E0B";
  if (normalized === "AVOID") return "#EF4444";
  return "#64748B";
}

function windDegToArrow(deg?: number | null): string {
  if (deg == null) return "";
  const dirs = ["↑", "↗", "→", "↘", "↓", "↙", "←", "↖"];
  return dirs[Math.round(deg / 45) % 8];
}

function weatherColor(indicator?: string | null) {
  const w = (indicator || "").toLowerCase();
  if (w === "green") return { border: "#22C55E55", text: "#86EFAC" };
  if (w === "yellow") return { border: "#F59E0B55", text: "#FDE68A" };
  if (w === "red") return { border: "#EF444455", text: "#FCA5A5" };
  return { border: "#33415555", text: "#94A3B8" };
}

function formatOdds(ml?: number | null): string {
  if (ml == null) return "—";
  return ml > 0 ? `+${ml}` : `${ml}`;
}

function WeatherBadge({ indicator }: { indicator?: string | null }) {
  if (!indicator) return null;
  const { border, text } = weatherColor(indicator);
  const icon = indicator.toLowerCase() === "green" ? "☀" : indicator.toLowerCase() === "yellow" ? "⛅" : "🌧";
  return (
    <View style={[styles.tag, { borderColor: border }]}>
      <Text style={[styles.tagText, { color: text }]}>{icon} {indicator}</Text>
    </View>
  );
}

export function MlbUpcomingGamesScreen() {
  const router = useRouter();
  const { colors } = useTheme();
  const { data, loading, error, refetch } = useMlbUpcomingGames();
  const { data: debugData, loading: debugLoading, refetch: refetchDebug } = useMlbUpcomingDebug();

  return (
    <ScrollView style={styles.screen} contentContainerStyle={styles.content}>
      <View style={[styles.hero, { borderColor: colors.border.subtle }]}>
        <Text style={styles.eyebrow}>MLB MATCHUP ANALYTICS</Text>
        <Text style={styles.h1}>Upcoming Games</Text>
        <Text style={styles.sub}>
          Today&apos;s MLB games with start time and model status. Tap any game to open matchup details.
        </Text>
      </View>

      {loading ? <ActivityIndicator color="#22D3EE" /> : null}

      {error ? (
        <Pressable onPress={refetch} style={[styles.errorBox, { borderColor: colors.border.subtle }]}>
          <Text style={styles.errorTitle}>Failed to load MLB games.</Text>
          <Text style={styles.errorText}>{error}</Text>
          <Text style={styles.errorRetry}>Tap to retry</Text>
        </Pressable>
      ) : null}

      {(data ?? []).map((game) => {
        const gamePk = game.game_pk;
        const away = game.away_team ?? "Away";
        const home = game.home_team ?? "Home";
        const awayLogo = getMlbTeamLogo(away) ?? undefined;
        const homeLogo = getMlbTeamLogo(home) ?? undefined;
        const topGrade = game.top_grade ?? "Pending";
        const topColor = scoreColor(topGrade);
        const topScore = game.top_score != null ? game.top_score.toFixed(1) : "—";
        const hasData = Boolean(game.has_model_data);
        const venueLine = game.ballpark_name ?? game.venue_name;

        return (
          <Pressable
            key={String(gamePk)}
            onPress={() =>
              router.push({
                pathname: "/(tabs)/mlb/hr-matchup/[gamePk]" as any,
                params: {
                  gamePk: String(gamePk),
                  homeTeam: home,
                  awayTeam: away,
                  startTimeUtc: game.start_time_utc ?? "",
                },
              })
            }
            style={[styles.card, { borderColor: colors.border.subtle }]}
          >
            <View style={styles.slugRow}>
              <View style={styles.teamCol}>
                {awayLogo ? <Image source={{ uri: awayLogo }} style={styles.logo} /> : null}
                <Text style={styles.teamName} numberOfLines={1}>{away}</Text>
                {game.away_moneyline != null ? (
                  <Text style={styles.oddsText}>{formatOdds(game.away_moneyline)}</Text>
                ) : null}
              </View>
              <View style={styles.centerCol}>
                <Text style={[styles.time, { color: "#C7D2FE" }]}>{formatET(game.start_time_utc ?? null)} ET</Text>
                <Text style={styles.centerLabel}>at</Text>
                {game.over_under != null ? (
                  <Text style={styles.ouText}>O/U {game.over_under}</Text>
                ) : null}
              </View>
              <View style={styles.teamCol}>
                {homeLogo ? <Image source={{ uri: homeLogo }} style={styles.logo} /> : null}
                <Text style={styles.teamName} numberOfLines={1}>{home}</Text>
                {game.home_moneyline != null ? (
                  <Text style={styles.oddsText}>{formatOdds(game.home_moneyline)}</Text>
                ) : null}
              </View>
            </View>

            <Text style={[styles.meta, { color: colors.text.muted }]}>
              {venueLine ? `${venueLine} • ` : ""}
              {hasData ? `${game.picks_count ?? 0} picks` : "Model data pending"}
            </Text>

            {/* Weather detail line */}
            {game.game_temp != null || game.wind_speed != null ? (
              <Text style={styles.weatherLine}>
                {game.game_temp != null ? `${Math.round(game.game_temp)}°F` : ""}
                {game.wind_speed != null ? `  💨 ${game.wind_speed.toFixed(1)} mph${game.wind_dir != null ? ` ${windDegToArrow(game.wind_dir)}` : ""}` : ""}
                {game.conditions ? `  ${game.conditions}` : ""}
              </Text>
            ) : null}

            <View style={styles.tagRow}>
              <View style={[styles.tag, { borderColor: hasData ? "#22C55E55" : "#334155" }]}>
                <Text style={[styles.tagText, { color: hasData ? "#86EFAC" : "#94A3B8" }]}>
                  {hasData ? "Model Ready" : "Pending"}
                </Text>
              </View>
              <View style={[styles.tag, { borderColor: `${topColor}66` }]}>
                <Text style={[styles.tagText, { color: topColor }]}>
                  Top {topGrade} • {topScore}
                </Text>
              </View>
              <WeatherBadge indicator={game.weather_indicator} />
            </View>
          </Pressable>
        );
      })}

      {!loading && !error && (data ?? []).length === 0 ? (
        <>
          <View style={[styles.card, { borderColor: colors.border.subtle }]}>
            <Text style={[styles.matchup, { color: colors.text.primary }]}>No MLB games found today.</Text>
            <Text style={[styles.meta, { color: colors.text.muted }]}>
              Games will appear here once today&apos;s schedule is available.
            </Text>
          </View>

          <View style={[styles.debugCard, { borderColor: "#334155" }]}>
            <Text style={styles.debugTitle}>Debug</Text>
            {debugLoading ? <ActivityIndicator color="#64748B" size="small" /> : null}
            {!debugLoading ? (
              <>
                <Text style={styles.debugLine}>backend_now_et: {debugData?.now_et ?? "—"}</Text>
                <Text style={styles.debugLine}>today_et: {debugData?.today_et ?? "—"}</Text>
                <Text style={styles.debugLine}>tomorrow_et: {debugData?.tomorrow_et ?? "—"}</Text>
                <Text style={styles.debugLine}>schedule_today_count: {debugData?.today_schedule_count ?? 0}</Text>
                <Text style={styles.debugLine}>schedule_tomorrow_count: {debugData?.tomorrow_schedule_count ?? 0}</Text>
                <Text style={styles.debugLine}>combined_count: {debugData?.combined_schedule_count ?? 0}</Text>
                <Text style={styles.debugLine}>fetch_error: {debugData?.fetch_error ?? "none"}</Text>
              </>
            ) : null}
            <Pressable
              onPress={() => {
                refetch();
                refetchDebug();
              }}
              style={styles.debugButton}
            >
              <Text style={styles.debugButtonText}>Retry + Refresh Debug</Text>
            </Pressable>
          </View>
        </>
      ) : null}
    </ScrollView>
  );
}

const styles = StyleSheet.create({
  screen: { flex: 1, backgroundColor: "#050A18" },
  content: { padding: 16, gap: 10, paddingBottom: 40 },
  hero: {
    borderWidth: StyleSheet.hairlineWidth,
    borderRadius: 16,
    backgroundColor: "#071731",
    padding: 16,
    marginBottom: 8,
  },
  eyebrow: { color: "#90B3E9", fontSize: 11, fontWeight: "700" },
  h1: { color: "#E9F2FF", fontSize: 24, fontWeight: "800", marginTop: 8 },
  sub: { color: "#A7C0E8", marginTop: 6, lineHeight: 18, fontSize: 13 },
  card: {
    borderWidth: StyleSheet.hairlineWidth,
    borderRadius: 14,
    backgroundColor: "#0B1529",
    padding: 12,
    gap: 8,
  },
  slugRow: { flexDirection: "row", alignItems: "center", justifyContent: "space-between", gap: 10 },
  teamCol: { flex: 1, alignItems: "center", gap: 4 },
  centerCol: { alignItems: "center", justifyContent: "center", gap: 2 },
  logo: { width: 30, height: 30, borderRadius: 15, backgroundColor: "#111827" },
  teamName: { color: "#E5E7EB", fontSize: 13, fontWeight: "700" },
  oddsText: { color: "#93C5FD", fontSize: 11, fontWeight: "700" },
  ouText: { color: "#A5B4FC", fontSize: 10, fontWeight: "700", marginTop: 2 },
  centerLabel: { color: "#64748B", fontSize: 10, fontWeight: "700", textTransform: "uppercase" },
  matchup: { fontSize: 14, fontWeight: "800", flex: 1 },
  time: { fontSize: 12, fontWeight: "700" },
  meta: { fontSize: 12 },
  weatherLine: { color: "#94A3B8", fontSize: 11 },
  tagRow: { flexDirection: "row", gap: 8, flexWrap: "wrap" },
  tag: {
    borderWidth: StyleSheet.hairlineWidth,
    borderRadius: 999,
    paddingHorizontal: 10,
    paddingVertical: 5,
    backgroundColor: "#0F172A",
  },
  tagText: { fontSize: 11, fontWeight: "700" },
  errorBox: { borderWidth: StyleSheet.hairlineWidth, borderRadius: 12, backgroundColor: "#1F2937", padding: 12 },
  errorTitle: { color: "#FCA5A5", fontWeight: "700" },
  errorText: { color: "#FECACA", marginTop: 4, fontSize: 12 },
  errorRetry: { color: "#E5E7EB", marginTop: 8, fontSize: 12 },
  debugCard: {
    borderWidth: StyleSheet.hairlineWidth,
    borderRadius: 12,
    backgroundColor: "#0A1224",
    padding: 12,
    gap: 4,
  },
  debugTitle: { color: "#94A3B8", fontSize: 12, fontWeight: "800" },
  debugLine: { color: "#64748B", fontSize: 11 },
  debugButton: {
    marginTop: 8,
    borderWidth: StyleSheet.hairlineWidth,
    borderColor: "#334155",
    borderRadius: 8,
    paddingVertical: 8,
    alignItems: "center",
    backgroundColor: "#0F172A",
  },
  debugButtonText: { color: "#93C5FD", fontSize: 11, fontWeight: "700" },
});
