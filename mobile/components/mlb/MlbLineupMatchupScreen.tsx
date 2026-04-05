import { ActivityIndicator, Image, Pressable, ScrollView, StyleSheet, Text, View } from "react-native";
import { useLocalSearchParams, useRouter } from "expo-router";

import { useMlbBattingOrder, type BattingOrderPosition } from "@/hooks/mlb/useMlbMatchups";
import { useTheme } from "@/store/useTheme";
import { getMlbTeamLogo } from "@/utils/mlbLogos";

function fmt(value?: number | null, digits = 1): string {
  if (value == null || !Number.isFinite(value)) return "—";
  return Number.isInteger(value) ? `${value}` : value.toFixed(digits);
}

function fmtOdds(value?: number | null): string {
  if (value == null || !Number.isFinite(value) || value === 0) return "—";
  return value > 0 ? `+${Math.round(value)}` : `${Math.round(value)}`;
}

function spotBg(pos: BattingOrderPosition) {
  if (pos.is_weak_spot) return { bg: "rgba(16,185,129,0.15)", border: "#10B981", text: "#A7F3D0" };
  return { bg: "transparent", border: "#374151", text: "#E5E7EB" };
}

export function MlbLineupMatchupScreen() {
  const { gamePk: rawGamePk, homeTeam, awayTeam } = useLocalSearchParams<{
    gamePk: string;
    homeTeam?: string;
    awayTeam?: string;
  }>();
  const gamePk = rawGamePk ? Number(rawGamePk) : null;
  const { data, loading, error, refetch } = useMlbBattingOrder(gamePk);
  const { colors } = useTheme();
  const router = useRouter();

  const game = data?.game;
  const homeTeamName = game?.home_team ?? homeTeam ?? "Home";
  const awayTeamName = game?.away_team ?? awayTeam ?? "Away";
  const homeLogo = getMlbTeamLogo(homeTeamName);
  const awayLogo = getMlbTeamLogo(awayTeamName);

  return (
    <ScrollView style={s.screen} contentContainerStyle={s.content}>
      {/* ── Navigation ── */}
      <View style={s.navRow}>
        <Pressable onPress={() => router.push("/(tabs)/mlb" as any)} style={s.navBtn}>
          <Text style={s.navBtnText}>← MLB</Text>
        </Pressable>
        <Pressable
          onPress={() => router.push({ pathname: "/(tabs)/mlb/pitching-props/[gamePk]" as any, params: { gamePk: String(gamePk), homeTeam: homeTeamName, awayTeam: awayTeamName } })}
          style={s.navBtn}
        >
          <Text style={s.navBtnText}>Pitching Props</Text>
        </Pressable>
        <Pressable onPress={() => router.push("/(tabs)/home")} style={s.navBtn}>
          <Text style={s.navBtnText}>Home</Text>
        </Pressable>
      </View>

      {/* ── Sub-tab indicator ── */}
      <View style={s.tabRow}>
        <Pressable
          onPress={() => router.push({ pathname: "/(tabs)/mlb/pitching-props/[gamePk]" as any, params: { gamePk: String(gamePk), homeTeam: homeTeamName, awayTeam: awayTeamName } })}
          style={s.tabInactive}
        >
          <Text style={s.tabTextInactive}>Pitching</Text>
        </Pressable>
        <View style={s.tabActive}>
          <Text style={s.tabTextActive}>Lineup</Text>
        </View>
      </View>

      {/* ── Hero ── */}
      <View style={[s.hero, { borderColor: colors.border.subtle }]}>
        <Text style={s.eyebrow}>PITCHER vs LINEUP POSITION</Text>
        <View style={s.slugRow}>
          <View style={s.heroTeamCol}>
            {awayLogo ? <Image source={{ uri: awayLogo }} style={s.heroLogo} /> : <View style={s.heroLogo} />}
            <Text style={s.heroTeamName} numberOfLines={2}>{awayTeamName}</Text>
          </View>
          <View style={s.heroCenterCol}>
            <Text style={s.slugTime}>
              {game?.start_time_utc ? new Date(game.start_time_utc).toLocaleTimeString() : "TBD"}
            </Text>
            <Text style={s.slugMeta}>{game?.venue_name ?? "Venue TBD"}</Text>
          </View>
          <View style={s.heroTeamCol}>
            {homeLogo ? <Image source={{ uri: homeLogo }} style={s.heroLogo} /> : <View style={s.heroLogo} />}
            <Text style={s.heroTeamName} numberOfLines={2}>{homeTeamName}</Text>
          </View>
        </View>
        <View style={s.tagRow}>
          <View style={s.pill}>
            <Text style={s.pillText}>
              💰 ML {fmtOdds(game?.odds?.away_moneyline)} / {fmtOdds(game?.odds?.home_moneyline)}{" "}
              {game?.odds?.over_under != null ? `• O/U ${game.odds.over_under}` : ""}
            </Text>
          </View>
        </View>
      </View>

      {loading ? <ActivityIndicator color="#93C5FD" /> : null}

      {error ? (
        <Pressable onPress={refetch} style={[s.errorBox, { borderColor: colors.border.subtle }]}>
          <Text style={s.errorTitle}>Failed to load lineup matchup.</Text>
          <Text style={s.errorText}>{error}</Text>
          <Text style={s.errorRetry}>Tap to retry</Text>
        </Pressable>
      ) : null}

      {/* ── Pitcher sections ── */}
      {(data?.pitchers ?? []).map((pitcher) => (
        <View key={String(pitcher.pitcher_id)} style={[s.panel, { borderColor: colors.border.subtle }]}>
          <View style={s.pitcherHeader}>
            <View style={{ flex: 1 }}>
              <Text style={s.pitcherName}>{pitcher.pitcher_name ?? "Pitcher"}</Text>
              <Text style={s.pitcherSub}>
                {pitcher.pitcher_hand ?? "RHP"} • vs {pitcher.offense_team ?? "Offense"}
              </Text>
            </View>
            <View style={s.weakBadge}>
              <Text style={s.weakBadgeText}>
                {pitcher.weak_spot_count ?? 0} Weak Spot{(pitcher.weak_spot_count ?? 0) !== 1 ? "s" : ""}
              </Text>
            </View>
          </View>

          {!pitcher.lineup_confirmed ? (
            <View style={s.lineupPending}>
              <Text style={s.lineupPendingText}>Lineup not yet confirmed — positions show historical data only</Text>
            </View>
          ) : null}

          {/* Legend */}
          <View style={s.legendRow}>
            <View style={[s.legendDot, { backgroundColor: "#10B981" }]} />
            <Text style={s.legendText}>= Weak Spot (OPS ≥ .780)</Text>
          </View>

          {/* Position table header */}
          <View style={s.tableHeader}>
            <Text style={[s.th, s.thSpot]}>#</Text>
            <Text style={[s.th, s.thPlayer]}>PLAYER</Text>
            <Text style={s.th}>AB</Text>
            <Text style={s.th}>H</Text>
            <Text style={s.th}>HR</Text>
            <Text style={s.th}>AVG</Text>
            <Text style={s.th}>OPS</Text>
          </View>

          {/* Position rows */}
          {(pitcher.positions ?? []).map((pos) => {
            const tone = spotBg(pos);
            return (
              <View
                key={`pos-${pos.batting_order}`}
                style={[s.posRow, { backgroundColor: tone.bg, borderColor: tone.border }]}
              >
                <View style={[s.td, s.thSpot]}>
                  <Text style={[s.spotNum, pos.is_weak_spot ? s.spotWeak : null]}>
                    {pos.batting_order}
                  </Text>
                </View>
                <View style={[s.td, s.thPlayer]}>
                  <Text style={[s.playerName, { color: tone.text }]} numberOfLines={1}>
                    {pos.player_name ?? "(TBD)"}
                  </Text>
                  {pos.is_weak_spot ? <Text style={s.weakLabel}>WEAK SPOT</Text> : null}
                </View>
                <Text style={[s.td, s.tdVal]}>{pos.at_bats ?? "—"}</Text>
                <Text style={[s.td, s.tdVal]}>{pos.hits ?? "—"}</Text>
                <Text style={[s.td, s.tdVal, pos.home_runs && pos.home_runs > 0 ? s.hrHighlight : null]}>
                  {pos.home_runs ?? "—"}
                </Text>
                <Text style={[s.td, s.tdVal]}>{fmt(pos.avg, 3)}</Text>
                <Text style={[s.td, s.tdVal, pos.is_weak_spot ? s.opsWeak : null]}>
                  {fmt(pos.ops, 3)}
                </Text>
              </View>
            );
          })}
        </View>
      ))}

      {!loading && !error && !(data?.pitchers?.length) ? (
        <View style={[s.panel, { borderColor: colors.border.subtle }]}>
          <Text style={s.emptyTitle}>No batting order data for this game yet.</Text>
          <Text style={s.emptySub}>Data populates once the ingest runs.</Text>
        </View>
      ) : null}
    </ScrollView>
  );
}

const s = StyleSheet.create({
  screen: { flex: 1, backgroundColor: "#050A18" },
  content: { padding: 16, gap: 10, paddingBottom: 40 },

  navRow: { flexDirection: "row", gap: 6, marginBottom: 2, flexWrap: "wrap" },
  navBtn: { borderWidth: StyleSheet.hairlineWidth, borderColor: "#334155", borderRadius: 8, paddingHorizontal: 10, paddingVertical: 6, backgroundColor: "#0F172A" },
  navBtnText: { color: "#93C5FD", fontSize: 11, fontWeight: "700" },

  tabRow: { flexDirection: "row", gap: 0, marginBottom: 4 },
  tabActive: { flex: 1, borderBottomWidth: 2, borderBottomColor: "#10B981", paddingVertical: 10, alignItems: "center" },
  tabInactive: { flex: 1, borderBottomWidth: 2, borderBottomColor: "#1E293B", paddingVertical: 10, alignItems: "center" },
  tabTextActive: { color: "#10B981", fontSize: 12, fontWeight: "800" },
  tabTextInactive: { color: "#64748B", fontSize: 12, fontWeight: "700" },

  hero: { borderWidth: StyleSheet.hairlineWidth, borderRadius: 16, backgroundColor: "#071731", padding: 16, gap: 8 },
  eyebrow: { color: "#10B981", fontSize: 11, fontWeight: "700" },
  slugRow: { flexDirection: "row", alignItems: "center", justifyContent: "space-between", gap: 12 },
  heroTeamCol: { flex: 1.25, alignItems: "center", gap: 6 },
  heroCenterCol: { flex: 1.8, alignItems: "center", gap: 3 },
  heroLogo: { width: 36, height: 36, borderRadius: 18, backgroundColor: "#111827" },
  heroTeamName: { color: "#E5E7EB", fontSize: 14, fontWeight: "800", textAlign: "center" },
  slugTime: { color: "#F8FAFC", fontSize: 18, fontWeight: "800" },
  slugMeta: { color: "#A7C0E8", fontSize: 11 },
  tagRow: { flexDirection: "row", flexWrap: "wrap", gap: 8, marginTop: 4 },
  pill: { borderWidth: StyleSheet.hairlineWidth, borderColor: "#334155", borderRadius: 999, backgroundColor: "#0F172A", paddingHorizontal: 10, paddingVertical: 6 },
  pillText: { color: "#BFDBFE", fontSize: 11, fontWeight: "700" },

  panel: { borderWidth: StyleSheet.hairlineWidth, borderRadius: 14, backgroundColor: "#0B1529", padding: 12, gap: 8 },
  pitcherHeader: { flexDirection: "row", alignItems: "center", justifyContent: "space-between", gap: 10 },
  pitcherName: { color: "#E5E7EB", fontSize: 18, fontWeight: "800" },
  pitcherSub: { color: "#94A3B8", fontSize: 12, marginTop: 2 },

  weakBadge: { borderWidth: 1, borderColor: "#10B981", borderRadius: 999, paddingHorizontal: 10, paddingVertical: 4, backgroundColor: "rgba(16,185,129,0.12)" },
  weakBadgeText: { color: "#A7F3D0", fontSize: 11, fontWeight: "800" },

  lineupPending: { backgroundColor: "rgba(245,158,11,0.1)", borderRadius: 8, padding: 8 },
  lineupPendingText: { color: "#FDE68A", fontSize: 11, fontWeight: "600", textAlign: "center" },

  legendRow: { flexDirection: "row", alignItems: "center", gap: 6 },
  legendDot: { width: 10, height: 10, borderRadius: 5 },
  legendText: { color: "#64748B", fontSize: 10, fontWeight: "600" },

  tableHeader: { flexDirection: "row", alignItems: "center", borderBottomWidth: StyleSheet.hairlineWidth, borderBottomColor: "#334155", paddingBottom: 6 },
  th: { flex: 1, color: "#64748B", fontSize: 10, fontWeight: "800", textAlign: "center" },
  thSpot: { flex: 0.5, textAlign: "center" },
  thPlayer: { flex: 2.5, textAlign: "left", paddingLeft: 4 },

  posRow: { flexDirection: "row", alignItems: "center", borderWidth: 1, borderRadius: 8, paddingVertical: 8, paddingHorizontal: 4, marginTop: 4 },
  td: { flex: 1, alignItems: "center" },
  tdVal: { color: "#E5E7EB", fontSize: 12, fontWeight: "600", textAlign: "center", flex: 1 },

  spotNum: { color: "#94A3B8", fontSize: 14, fontWeight: "800" },
  spotWeak: { color: "#10B981" },
  playerName: { fontSize: 12, fontWeight: "700" },
  weakLabel: { color: "#10B981", fontSize: 8, fontWeight: "800", marginTop: 1 },
  hrHighlight: { color: "#F59E0B" },
  opsWeak: { color: "#10B981", fontWeight: "800" },

  errorBox: { borderWidth: StyleSheet.hairlineWidth, borderRadius: 12, padding: 16, gap: 6, backgroundColor: "#0B1529" },
  errorTitle: { color: "#F87171", fontSize: 14, fontWeight: "800" },
  errorText: { color: "#94A3B8", fontSize: 12 },
  errorRetry: { color: "#93C5FD", fontSize: 12, fontWeight: "700" },
  emptyTitle: { color: "#E5E7EB", fontSize: 15, fontWeight: "700" },
  emptySub: { color: "#94A3B8", fontSize: 12 },
});
