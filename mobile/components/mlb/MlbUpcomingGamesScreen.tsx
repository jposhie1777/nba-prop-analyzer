import { ActivityIndicator, Image, Pressable, ScrollView, StyleSheet, Text, View } from "react-native";
import { useRouter } from "expo-router";

import { useMlbUpcomingDebug, useMlbUpcomingGames, type MlbUpcomingGame } from "@/hooks/mlb/useMlbMatchups";
import { useTheme } from "@/store/useTheme";
import { formatET } from "@/lib/time/formatET";
import { getMlbTeamLogo } from "@/utils/mlbLogos";

// ── Helpers ────────────────────────────────────────────────────────────────

function windDegToLabel(deg?: number | null): string {
  if (deg == null) return "—";
  const dirs = ["N", "NNE", "NE", "ENE", "E", "ESE", "SE", "SSE", "S", "SSW", "SW", "WSW", "W", "WNW", "NW", "NNW"];
  return dirs[Math.round(deg / 22.5) % 16];
}

function windDegToArrow(deg?: number | null): string {
  if (deg == null) return "";
  // Wind direction is where wind comes FROM — arrow points the direction it blows TO
  const arrows = ["↓", "↙", "←", "↖", "↑", "↗", "→", "↘"];
  return arrows[Math.round(deg / 45) % 8];
}

function weatherIcon(conditions?: string | null, indicator?: string | null): string {
  const c = (conditions ?? "").toLowerCase();
  if (c.includes("rain") || c.includes("drizzle") || c.includes("shower")) return "🌧";
  if (c.includes("thunder") || c.includes("storm")) return "⛈";
  if (c.includes("snow") || c.includes("sleet")) return "🌨";
  if (c.includes("overcast")) return "☁️";
  if (c.includes("cloud") || c.includes("partly")) return "⛅";
  if (c.includes("fog") || c.includes("mist") || c.includes("haze")) return "🌫";
  const ind = (indicator ?? "").toLowerCase();
  if (ind === "red") return "🌧";
  if (ind === "yellow") return "⛅";
  return "☀️";
}

function indicatorColor(indicator?: string | null) {
  const w = (indicator ?? "").toLowerCase();
  if (w === "green") return { bg: "rgba(34,197,94,0.12)", border: "#22C55E", text: "#86EFAC" };
  if (w === "yellow") return { bg: "rgba(245,158,11,0.12)", border: "#F59E0B", text: "#FDE68A" };
  if (w === "red") return { bg: "rgba(239,68,68,0.12)", border: "#EF4444", text: "#FCA5A5" };
  return { bg: "rgba(148,163,184,0.08)", border: "#334155", text: "#94A3B8" };
}

function formatOdds(ml?: number | null): string {
  if (ml == null) return "—";
  return ml > 0 ? `+${ml}` : `${ml}`;
}

function scoreColor(grade?: string | null) {
  const g = (grade ?? "").toUpperCase();
  if (g === "IDEAL") return "#22C55E";
  if (g === "FAVORABLE") return "#10B981";
  if (g === "AVERAGE") return "#F59E0B";
  if (g === "AVOID") return "#EF4444";
  return "#64748B";
}

// ── Weather row in table ───────────────────────────────────────────────────

function WeatherRow({ label, value, highlight }: { label: string; value: string; highlight?: boolean }) {
  return (
    <View style={s.wxRow}>
      <Text style={s.wxLabel}>{label}</Text>
      <Text style={[s.wxValue, highlight ? s.wxValueHighlight : null]}>{value}</Text>
    </View>
  );
}

// ── Single game card ───────────────────────────────────────────────────────

function GameCard({ game }: { game: MlbUpcomingGame }) {
  const router = useRouter();
  const { colors } = useTheme();
  const away = game.away_team ?? "Away";
  const home = game.home_team ?? "Home";
  const awayLogo = getMlbTeamLogo(away) ?? undefined;
  const homeLogo = getMlbTeamLogo(home) ?? undefined;
  const venue = game.ballpark_name ?? game.venue_name;
  const hasData = Boolean(game.has_model_data);
  const topGrade = game.top_grade ?? "Pending";
  const topColor = scoreColor(topGrade);
  const indColor = indicatorColor(game.weather_indicator);
  const icon = weatherIcon(game.conditions, game.weather_indicator);

  return (
    <Pressable
      onPress={() =>
        router.push({
          pathname: "/(tabs)/mlb/pitching-props/[gamePk]" as any,
          params: {
            gamePk: String(game.game_pk),
            homeTeam: home,
            awayTeam: away,
            startTimeUtc: game.start_time_utc ?? "",
          },
        })
      }
      style={[s.card, { borderColor: colors.border.subtle }]}
    >
      {/* ── Game header: Away @ Home ── */}
      <View style={s.gameHeader}>
        <View style={s.gameHeaderTeams}>
          {awayLogo ? <Image source={{ uri: awayLogo }} style={s.headerLogo} /> : null}
          <Text style={s.headerTeamName}>{away}</Text>
          <Text style={s.headerAt}>@</Text>
          <Text style={s.headerTeamName}>{home}</Text>
          {homeLogo ? <Image source={{ uri: homeLogo }} style={s.headerLogo} /> : null}
        </View>

        {/* Venue + time + indicator badge */}
        <View style={s.venueLine}>
          <Text style={s.venueText}>{venue ?? "Venue TBD"}</Text>
          <Text style={s.venueSep}>|</Text>
          <Text style={s.venueTime}>{formatET(game.start_time_utc ?? null)} ET</Text>
          {game.weather_indicator ? (
            <View style={[s.indicatorBadge, { backgroundColor: indColor.bg, borderColor: indColor.border }]}>
              <Text style={[s.indicatorText, { color: indColor.text }]}>
                {game.weather_indicator}
              </Text>
            </View>
          ) : null}
        </View>

        {/* Weather note (delay warning etc) */}
        {game.weather_note ? (
          <View style={s.weatherNoteInline}>
            <Text style={s.weatherNoteInlineText} numberOfLines={2}>
              {game.weather_note}
            </Text>
          </View>
        ) : null}
      </View>

      {/* ── Weather data table ── */}
      <View style={s.wxTable}>
        {/* Conditions row with icon */}
        <View style={s.wxRow}>
          <Text style={s.wxLabel}>Conditions</Text>
          <View style={s.wxConditionsCell}>
            <Text style={s.wxCondIcon}>{icon}</Text>
            <Text style={s.wxCondText}>{game.conditions ?? "—"}</Text>
          </View>
        </View>

        <WeatherRow
          label="Temperature"
          value={game.game_temp != null ? `${Math.round(game.game_temp)}°F` : "—"}
          highlight={game.game_temp != null && game.game_temp >= 45}
        />

        <WeatherRow
          label="Precipitation"
          value={game.precip_prob != null ? `${Math.round(game.precip_prob)}%` : "—"}
          highlight={game.precip_prob != null && game.precip_prob <= 10}
        />

        <WeatherRow
          label="Wind Speed"
          value={game.wind_speed != null ? `${game.wind_speed.toFixed(1)} MPH` : "—"}
        />

        {/* Wind direction with arrow */}
        <View style={s.wxRow}>
          <Text style={s.wxLabel}>Wind Dir</Text>
          <View style={s.wxWindCell}>
            <Text style={s.wxWindArrow}>{windDegToArrow(game.wind_dir)}</Text>
            <Text style={s.wxValue}>{windDegToLabel(game.wind_dir)}</Text>
          </View>
        </View>

        {game.roof_type && game.roof_type.toLowerCase() !== "open" ? (
          <WeatherRow label="Roof" value={game.roof_type} />
        ) : null}
      </View>

      {/* ── Odds + Model status row ── */}
      <View style={s.bottomRow}>
        <View style={s.oddsGroup}>
          {game.away_moneyline != null ? (
            <Text style={s.oddsChip}>{away} {formatOdds(game.away_moneyline)}</Text>
          ) : null}
          {game.home_moneyline != null ? (
            <Text style={s.oddsChip}>{home} {formatOdds(game.home_moneyline)}</Text>
          ) : null}
          {game.over_under != null ? (
            <Text style={s.oddsChip}>O/U {game.over_under}</Text>
          ) : null}
        </View>
        <View style={s.modelGroup}>
          <View style={[s.modelBadge, { borderColor: hasData ? "#22C55E55" : "#334155" }]}>
            <Text style={[s.modelBadgeText, { color: hasData ? "#86EFAC" : "#94A3B8" }]}>
              {hasData ? `${game.picks_count ?? 0} picks` : "Pending"}
            </Text>
          </View>
          {hasData ? (
            <View style={[s.modelBadge, { borderColor: `${topColor}66` }]}>
              <Text style={[s.modelBadgeText, { color: topColor }]}>
                Top: {topGrade} {game.top_score != null ? game.top_score.toFixed(1) : ""}
              </Text>
            </View>
          ) : null}
        </View>
      </View>

      {/* ── Pitchers ── */}
      {(game.away_pitcher_name || game.home_pitcher_name) ? (
        <View style={s.pitcherRow}>
          <Text style={s.pitcherLabel}>
            {game.away_pitcher_name ?? "TBD"} vs {game.home_pitcher_name ?? "TBD"}
          </Text>
        </View>
      ) : null}
    </Pressable>
  );
}

// ── Main screen ────────────────────────────────────────────────────────────

export function MlbUpcomingGamesScreen() {
  const { colors } = useTheme();
  const { data, loading, error, refetch } = useMlbUpcomingGames();
  const { data: debugData, loading: debugLoading, refetch: refetchDebug } = useMlbUpcomingDebug();

  return (
    <ScrollView style={s.screen} contentContainerStyle={s.content}>
      {loading ? <ActivityIndicator color="#22D3EE" /> : null}

      {error ? (
        <Pressable onPress={refetch} style={[s.errorBox, { borderColor: colors.border.subtle }]}>
          <Text style={s.errorTitle}>Failed to load MLB games.</Text>
          <Text style={s.errorText}>{error}</Text>
          <Text style={s.errorRetry}>Tap to retry</Text>
        </Pressable>
      ) : null}

      {(data ?? []).map((game) => (
        <GameCard key={String(game.game_pk)} game={game} />
      ))}

      {!loading && !error && (data ?? []).length === 0 ? (
        <>
          <View style={[s.card, { borderColor: colors.border.subtle }]}>
            <Text style={s.emptyTitle}>No MLB games found today.</Text>
            <Text style={s.emptyText}>
              Games will appear here once today&apos;s schedule is available.
            </Text>
          </View>

          <View style={[s.debugCard, { borderColor: "#334155" }]}>
            <Text style={s.debugTitle}>Debug</Text>
            {debugLoading ? <ActivityIndicator color="#64748B" size="small" /> : null}
            {!debugLoading ? (
              <>
                <Text style={s.debugLine}>backend_now_et: {debugData?.now_et ?? "—"}</Text>
                <Text style={s.debugLine}>today_et: {debugData?.today_et ?? "—"}</Text>
                <Text style={s.debugLine}>tomorrow_et: {debugData?.tomorrow_et ?? "—"}</Text>
                <Text style={s.debugLine}>schedule_today_count: {debugData?.today_schedule_count ?? 0}</Text>
                <Text style={s.debugLine}>schedule_tomorrow_count: {debugData?.tomorrow_schedule_count ?? 0}</Text>
                <Text style={s.debugLine}>combined_count: {debugData?.combined_schedule_count ?? 0}</Text>
                <Text style={s.debugLine}>fetch_error: {debugData?.fetch_error ?? "none"}</Text>
              </>
            ) : null}
            <Pressable
              onPress={() => {
                refetch();
                refetchDebug();
              }}
              style={s.debugButton}
            >
              <Text style={s.debugButtonText}>Retry + Refresh Debug</Text>
            </Pressable>
          </View>
        </>
      ) : null}
    </ScrollView>
  );
}

// ── Styles ──────────────────────────────────────────────────────────────────

const s = StyleSheet.create({
  screen: { flex: 1, backgroundColor: "#050A18" },
  content: { padding: 12, gap: 16, paddingBottom: 40 },

  // Card
  card: {
    borderWidth: StyleSheet.hairlineWidth,
    borderRadius: 14,
    backgroundColor: "#0B1529",
    overflow: "hidden",
  },

  // Game header
  gameHeader: {
    padding: 14,
    gap: 6,
    borderBottomWidth: StyleSheet.hairlineWidth,
    borderBottomColor: "#1E293B",
    alignItems: "center",
  },
  gameHeaderTeams: {
    flexDirection: "row",
    alignItems: "center",
    justifyContent: "center",
    gap: 8,
  },
  headerLogo: { width: 28, height: 28, borderRadius: 14, backgroundColor: "#111827" },
  headerTeamName: { color: "#E5E7EB", fontSize: 16, fontWeight: "800" },
  headerAt: { color: "#64748B", fontSize: 13, fontWeight: "700" },
  venueLine: {
    flexDirection: "row",
    alignItems: "center",
    gap: 6,
    flexWrap: "wrap",
    justifyContent: "center",
  },
  venueText: { color: "#94A3B8", fontSize: 11 },
  venueSep: { color: "#334155", fontSize: 11 },
  venueTime: { color: "#CBD5E1", fontSize: 11, fontWeight: "700" },
  indicatorBadge: {
    borderWidth: 1,
    borderRadius: 999,
    paddingHorizontal: 8,
    paddingVertical: 2,
  },
  indicatorText: { fontSize: 10, fontWeight: "800" },
  weatherNoteInline: {
    backgroundColor: "rgba(245,158,11,0.08)",
    borderRadius: 6,
    paddingHorizontal: 10,
    paddingVertical: 4,
    marginTop: 2,
  },
  weatherNoteInlineText: { color: "#FDE68A", fontSize: 10, fontWeight: "600", textAlign: "center" },

  // Weather table
  wxTable: {
    paddingHorizontal: 14,
    paddingVertical: 8,
    gap: 0,
    borderBottomWidth: StyleSheet.hairlineWidth,
    borderBottomColor: "#1E293B",
  },
  wxRow: {
    flexDirection: "row",
    alignItems: "center",
    justifyContent: "space-between",
    paddingVertical: 6,
    borderBottomWidth: StyleSheet.hairlineWidth,
    borderBottomColor: "rgba(30,41,59,0.5)",
  },
  wxLabel: { color: "#64748B", fontSize: 11, fontWeight: "700", width: 100 },
  wxValue: { color: "#CBD5E1", fontSize: 12, fontWeight: "700", textAlign: "right", flex: 1 },
  wxValueHighlight: { color: "#86EFAC" },
  wxConditionsCell: {
    flexDirection: "row",
    alignItems: "center",
    gap: 6,
    flex: 1,
    justifyContent: "flex-end",
  },
  wxCondIcon: { fontSize: 16 },
  wxCondText: { color: "#CBD5E1", fontSize: 11, fontWeight: "600" },
  wxWindCell: {
    flexDirection: "row",
    alignItems: "center",
    gap: 6,
    flex: 1,
    justifyContent: "flex-end",
  },
  wxWindArrow: { color: "#F59E0B", fontSize: 18, fontWeight: "800" },

  // Bottom row: odds + model
  bottomRow: {
    flexDirection: "row",
    alignItems: "center",
    justifyContent: "space-between",
    paddingHorizontal: 14,
    paddingVertical: 8,
    gap: 8,
    flexWrap: "wrap",
  },
  oddsGroup: { flexDirection: "row", gap: 6, flexWrap: "wrap" },
  oddsChip: { color: "#93C5FD", fontSize: 11, fontWeight: "700" },
  modelGroup: { flexDirection: "row", gap: 6 },
  modelBadge: {
    borderWidth: StyleSheet.hairlineWidth,
    borderRadius: 999,
    paddingHorizontal: 8,
    paddingVertical: 3,
    backgroundColor: "#0F172A",
  },
  modelBadgeText: { fontSize: 10, fontWeight: "700" },

  // Pitcher row
  pitcherRow: {
    paddingHorizontal: 14,
    paddingVertical: 6,
    borderTopWidth: StyleSheet.hairlineWidth,
    borderTopColor: "#1E293B",
  },
  pitcherLabel: { color: "#94A3B8", fontSize: 11, fontWeight: "600", textAlign: "center" },

  // Error / empty / debug
  emptyTitle: { color: "#E5E7EB", fontSize: 14, fontWeight: "700", padding: 14 },
  emptyText: { color: "#94A3B8", fontSize: 12, paddingHorizontal: 14, paddingBottom: 14 },
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
