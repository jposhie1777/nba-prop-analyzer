import { useCallback, useEffect, useMemo, useState } from "react";
import {
  ActivityIndicator,
  Image,
  Linking,
  Pressable,
  ScrollView,
  StyleSheet,
  Text,
  View,
} from "react-native";

import { useMlbNrfi, type NrfiMatchup, type NrfiTeam } from "@/hooks/mlb/useMlbMatchups";
import { useTheme } from "@/store/useTheme";
import { usePropBetslip } from "@/store/usePropBetslip";
import { useUserSettings, FD_STATES } from "@/store/useUserSettings";
import { buildFanDuelParlay, getBuildPlatform } from "@/utils/parlayBuilder";
import { getMlbTeamLogo } from "@/utils/mlbLogos";
import { formatET } from "@/lib/time/formatET";

// ── Helpers ────────────────────────────────────────────────────────────────

function scoreColor(score?: number | null): string {
  if (score == null) return "#64748B";
  if (score >= 80) return "#10B981";
  if (score >= 70) return "#22D3EE";
  if (score >= 60) return "#F59E0B";
  return "#EF4444";
}

function streakColor(streak?: string | null): string {
  if (!streak) return "#64748B";
  const s = streak.toUpperCase();
  if (s.includes("NRFI")) return "#10B981";
  if (s.includes("YRFI")) return "#EF4444";
  return "#64748B";
}

function streakBadgeBg(streak?: string | null): string {
  if (!streak) return "#0F172A";
  const s = streak.toUpperCase();
  if (s.includes("NRFI")) return "rgba(16,185,129,0.12)";
  if (s.includes("YRFI")) return "rgba(239,68,68,0.12)";
  return "#0F172A";
}

function fmtOdds(value?: number | null): string {
  if (value == null || !Number.isFinite(value) || value === 0) return "—";
  return value > 0 ? `+${Math.round(value)}` : `${Math.round(value)}`;
}

/** Build a FanDuel game page URL for a matchup */
function buildFdGameUrl(matchup: NrfiMatchup, platform: string): string {
  const away = (matchup.away_team.team_name ?? "away").toLowerCase().replace(/\s+/g, "-");
  const home = (matchup.home_team.team_name ?? "home").toLowerCase().replace(/\s+/g, "-");
  const base = platform === "ios" || platform === "android"
    ? "fanduelsportsbook://account.sportsbook.fanduel.com/sportsbook/navigation/mlb"
    : "https://sportsbook.fanduel.com/baseball/mlb";
  return base;
}

// ── Team row ───────────────────────────────────────────────────────────────

function NrfiTeamRow({ team }: { team: NrfiTeam }) {
  const logo = getMlbTeamLogo(team.team_name ?? "") ?? undefined;

  return (
    <View style={st.teamRow}>
      <View style={st.teamInfoCol}>
        {logo ? <Image source={{ uri: logo }} style={st.teamLogo} /> : <View style={st.teamLogoPlaceholder} />}
        <View style={st.teamNameWrap}>
          <Text style={st.teamName} numberOfLines={1}>{team.team_name ?? "—"}</Text>
        </View>
      </View>

      <View style={st.statCol}>
        <Text style={st.statValue}>{team.team_nrfi_record ?? "—"}</Text>
      </View>

      <View style={st.statColNarrow}>
        <Text style={st.statValue}>{team.team_l10_record ?? "—"}</Text>
      </View>

      <View style={st.pitcherCol}>
        <Text style={st.pitcherName} numberOfLines={1}>{team.pitcher_name ?? "TBD"}</Text>
      </View>

      <View style={st.statCol}>
        <Text style={st.statValue}>{team.pitcher_nrfi_record ?? "—"}</Text>
      </View>

      <View style={st.streakCol}>
        {team.team_streak ? (
          <View style={[st.streakBadge, { backgroundColor: streakBadgeBg(team.team_streak) }]}>
            <Text style={[st.streakText, { color: streakColor(team.team_streak) }]}>
              {team.team_streak}
            </Text>
          </View>
        ) : (
          <Text style={st.statValueMuted}>—</Text>
        )}
      </View>
    </View>
  );
}

// ── Matchup card ───────────────────────────────────────────────────────────

function NrfiMatchupCard({
  matchup,
  nrfiSelected,
  yrfiSelected,
  onToggleNrfi,
  onToggleYrfi,
}: {
  matchup: NrfiMatchup;
  nrfiSelected: boolean;
  yrfiSelected: boolean;
  onToggleNrfi: () => void;
  onToggleYrfi: () => void;
}) {
  const nrfiColor = scoreColor(matchup.nrfi_score);

  return (
    <View style={st.card}>
      <ScrollView horizontal showsHorizontalScrollIndicator={false}>
        <View style={st.cardInner}>
          {/* Column headers */}
          <View style={st.headerRow}>
            <View style={st.teamInfoCol}>
              <Text style={st.headerLabel}>MATCHUP</Text>
            </View>
            <View style={st.statCol}>
              <Text style={st.headerLabel}>NRFI-YRFI</Text>
            </View>
            <View style={st.statColNarrow}>
              <Text style={st.headerLabel}>L-10</Text>
            </View>
            <View style={st.pitcherCol}>
              <Text style={st.headerLabel}>PITCHER</Text>
            </View>
            <View style={st.statCol}>
              <Text style={st.headerLabel}>NRFI-YRFI</Text>
            </View>
            <View style={st.streakCol}>
              <Text style={st.headerLabel}>STREAK</Text>
            </View>
          </View>

          {/* Away team row */}
          <NrfiTeamRow team={matchup.away_team} />

          {/* Home team row */}
          <NrfiTeamRow team={matchup.home_team} />
        </View>
      </ScrollView>

      {/* Score + bet buttons row */}
      <View style={st.scoreRow}>
        <View style={st.scoreLeft}>
          <Text style={st.scoreLabel}>NRFI Score</Text>
          <Text style={[st.scoreValue, { color: nrfiColor }]}>
            {matchup.nrfi_score != null ? matchup.nrfi_score.toFixed(1) : "—"}
          </Text>
        </View>

        <View style={st.betBtnRow}>
          <Pressable
            style={[st.betBtn, st.nrfiBtn, nrfiSelected ? st.nrfiBtnActive : null]}
            onPress={onToggleNrfi}
          >
            <Text style={[st.betBtnText, nrfiSelected ? st.nrfiBtnTextActive : null]}>
              {nrfiSelected ? "✓ " : ""}NRFI
            </Text>
            {matchup.nrfi_odds != null ? (
              <Text style={[st.betBtnOdds, nrfiSelected ? st.nrfiBtnTextActive : null]}>
                {fmtOdds(matchup.nrfi_odds)}
              </Text>
            ) : null}
          </Pressable>
          <Pressable
            style={[st.betBtn, st.yrfiBtn, yrfiSelected ? st.yrfiBtnActive : null]}
            onPress={onToggleYrfi}
          >
            <Text style={[st.betBtnText, yrfiSelected ? st.yrfiBtnTextActive : null]}>
              {yrfiSelected ? "✓ " : ""}YRFI
            </Text>
            {matchup.yrfi_odds != null ? (
              <Text style={[st.betBtnOdds, yrfiSelected ? st.yrfiBtnTextActive : null]}>
                {fmtOdds(matchup.yrfi_odds)}
              </Text>
            ) : null}
          </Pressable>
        </View>
      </View>
    </View>
  );
}

// ── Main screen ────────────────────────────────────────────────────────────

export function MlbNrfiScreen() {
  const { colors } = useTheme();
  const fdState = useUserSettings((s) => s.fdState);
  const setFdState = useUserSettings((s) => s.setFdState);
  const hydrate = useUserSettings((s) => s.hydrate);
  const hydrated = useUserSettings((s) => s._hydrated);
  const { data, loading, error, refetch } = useMlbNrfi(fdState);
  const platform = getBuildPlatform();
  const [showStatePicker, setShowStatePicker] = useState(false);

  // Hydrate settings on mount
  useEffect(() => { hydrate(); }, [hydrate]);

  // Global betslip
  const slipItems = usePropBetslip((s) => s.items);
  const addToSlip = usePropBetslip((s) => s.add);
  const removeFromSlip = usePropBetslip((s) => s.remove);
  const clearSlip = usePropBetslip((s) => s.clear);

  // Derive selected NRFI/YRFI keys from betslip
  const selectedKeys = useMemo(() => {
    const keys = new Set<string>();
    for (const item of slipItems) {
      if (item.sport === "mlb" && (item.market === "NRFI" || item.market === "YRFI")) {
        keys.add(item.id);
      }
    }
    return keys;
  }, [slipItems]);

  const toggleNrfi = useCallback(
    (matchup: NrfiMatchup) => {
      const gameId = matchup.game_id ?? 0;
      const slipId = `mlb-nrfi-${gameId}`;
      const yrfiId = `mlb-yrfi-${gameId}`;

      if (selectedKeys.has(slipId)) {
        removeFromSlip(slipId);
      } else {
        // Remove YRFI for same game if selected
        if (selectedKeys.has(yrfiId)) removeFromSlip(yrfiId);
        addToSlip({
          id: slipId,
          player_id: gameId,
          player: `${matchup.away_team.team_code ?? "?"} @ ${matchup.home_team.team_code ?? "?"}`,
          market: "NRFI",
          side: "under",
          line: 0.5,
          odds: matchup.nrfi_odds ?? 0,
          sport: "mlb",
          bookmaker: "FanDuel",
          matchup: `${matchup.away_team.team_name ?? ""} @ ${matchup.home_team.team_name ?? ""}`,
          fd_market_id: matchup.fd_market_id ?? null,
          fd_selection_id: matchup.nrfi_selection_id ?? null,
        });
      }
    },
    [selectedKeys, addToSlip, removeFromSlip]
  );

  const toggleYrfi = useCallback(
    (matchup: NrfiMatchup) => {
      const gameId = matchup.game_id ?? 0;
      const slipId = `mlb-yrfi-${gameId}`;
      const nrfiId = `mlb-nrfi-${gameId}`;

      if (selectedKeys.has(slipId)) {
        removeFromSlip(slipId);
      } else {
        // Remove NRFI for same game if selected
        if (selectedKeys.has(nrfiId)) removeFromSlip(nrfiId);
        addToSlip({
          id: slipId,
          player_id: gameId,
          player: `${matchup.away_team.team_code ?? "?"} @ ${matchup.home_team.team_code ?? "?"}`,
          market: "YRFI",
          side: "over",
          line: 0.5,
          odds: matchup.yrfi_odds ?? 0,
          sport: "mlb",
          bookmaker: "FanDuel",
          matchup: `${matchup.away_team.team_name ?? ""} @ ${matchup.home_team.team_name ?? ""}`,
          fd_market_id: matchup.fd_market_id ?? null,
          fd_selection_id: matchup.yrfi_selection_id ?? null,
        });
      }
    },
    [selectedKeys, addToSlip, removeFromSlip]
  );

  const nrfiYrfiSlipItems = useMemo(
    () => slipItems.filter((i) => i.sport === "mlb" && (i.market === "NRFI" || i.market === "YRFI")),
    [slipItems]
  );

  function openUrl(url?: string | null) {
    if (!url) return;
    if (platform === "desktop" && typeof globalThis.open === "function") {
      globalThis.open(url, "_blank");
      return;
    }
    Linking.openURL(url).catch(() => {});
  }

  const fdUrl = useMemo(() => {
    if (nrfiYrfiSlipItems.length === 0) return null;
    return buildFanDuelParlay(
      nrfiYrfiSlipItems.map((i) => ({
        fd_market_id: i.fd_market_id ?? null,
        fd_selection_id: i.fd_selection_id ?? null,
      })),
      platform,
      fdState
    );
  }, [nrfiYrfiSlipItems, platform, fdState]);

  const matchups = data?.matchups ?? [];

  return (
    <View style={st.screen}>
      <ScrollView style={st.scrollView} contentContainerStyle={st.content}>
        <View style={[st.hero, { borderColor: colors.border.subtle }]}>
          <Text style={st.eyebrow}>NRFI / YRFI RESEARCH</Text>
          <Text style={st.h1}>First Inning Trends</Text>
          <Text style={st.sub}>
            No Run First Inning trends by team, pitcher, and today&apos;s matchups. Sorted by NRFI score. Tap NRFI or YRFI to add to betslip.
          </Text>

          {/* State picker row */}
          <Pressable style={st.stateRow} onPress={() => setShowStatePicker(!showStatePicker)}>
            <Text style={st.stateLabel}>FanDuel State:</Text>
            <Text style={st.stateValue}>
              {FD_STATES.find((s) => s.code === fdState)?.label ?? fdState.toUpperCase()}
            </Text>
            <Text style={st.stateChevron}>{showStatePicker ? "▲" : "▼"}</Text>
          </Pressable>

          {showStatePicker ? (
            <View style={st.stateGrid}>
              {FD_STATES.map((s) => (
                <Pressable
                  key={s.code}
                  style={[st.stateChip, fdState === s.code ? st.stateChipActive : null]}
                  onPress={() => { setFdState(s.code); setShowStatePicker(false); }}
                >
                  <Text style={[st.stateChipText, fdState === s.code ? st.stateChipTextActive : null]}>
                    {s.code.toUpperCase()}
                  </Text>
                </Pressable>
              ))}
            </View>
          ) : null}

          {matchups.length > 0 ? (
            <Text style={st.countText}>{matchups.length} matchups today</Text>
          ) : null}
        </View>

        {loading ? <ActivityIndicator color="#93C5FD" /> : null}

        {error ? (
          <Pressable onPress={refetch} style={[st.errorBox, { borderColor: colors.border.subtle }]}>
            <Text style={st.errorTitle}>Failed to load NRFI data.</Text>
            <Text style={st.errorText}>{error}</Text>
            <Text style={st.errorRetry}>Tap to retry</Text>
          </Pressable>
        ) : null}

        {matchups.map((matchup) => {
          const gameId = matchup.game_id ?? 0;
          return (
            <NrfiMatchupCard
              key={gameId}
              matchup={matchup}
              nrfiSelected={selectedKeys.has(`mlb-nrfi-${gameId}`)}
              yrfiSelected={selectedKeys.has(`mlb-yrfi-${gameId}`)}
              onToggleNrfi={() => toggleNrfi(matchup)}
              onToggleYrfi={() => toggleYrfi(matchup)}
            />
          );
        })}

        {!loading && !error && matchups.length === 0 ? (
          <View style={[st.emptyCard, { borderColor: colors.border.subtle }]}>
            <Text style={st.emptyTitle}>No NRFI data available today.</Text>
            <Text style={st.emptyText}>Data will appear once today&apos;s matchups are set.</Text>
          </View>
        ) : null}
      </ScrollView>

      {/* Fixed bottom parlay bar */}
      {nrfiYrfiSlipItems.length >= 1 ? (
        <View style={st.parlayBar}>
          <View style={st.parlayTopRow}>
            <Text style={st.parlayTitle}>
              {nrfiYrfiSlipItems.length} selection{nrfiYrfiSlipItems.length !== 1 ? "s" : ""}
            </Text>
            <Pressable onPress={clearSlip}>
              <Text style={st.parlayClear}>✕</Text>
            </Pressable>
          </View>

          <View style={st.parlayLegs}>
            {nrfiYrfiSlipItems.map((item) => (
              <Text key={item.id} style={st.parlayLegText}>
                {item.market === "NRFI" ? "🟢" : "🔴"} {item.player} — {item.market}
              </Text>
            ))}
          </View>

          <View style={st.parlayBtnRow}>
            <Pressable
              style={[st.parlayBtn, !fdUrl ? st.parlayBtnDisabled : null]}
              disabled={!fdUrl}
              onPress={() => openUrl(fdUrl)}
            >
              <Text style={st.parlayBtnText}>
                {nrfiYrfiSlipItems.length === 1 ? "Bet on FanDuel" : `Parlay on FanDuel (${nrfiYrfiSlipItems.length} legs)`}
              </Text>
            </Pressable>
          </View>

          <Text style={st.parlayNote}>
            Parlay availability subject to sportsbook approval.
          </Text>
        </View>
      ) : null}
    </View>
  );
}

// ── Styles ──────────────────────────────────────────────────────────────────

const st = StyleSheet.create({
  screen: { flex: 1, backgroundColor: "#050A18" },
  scrollView: { flex: 1 },
  content: { padding: 16, gap: 10, paddingBottom: 40 },

  // Hero
  hero: {
    borderWidth: StyleSheet.hairlineWidth,
    borderRadius: 16,
    backgroundColor: "#071731",
    padding: 16,
    gap: 6,
    marginBottom: 4,
  },
  eyebrow: { color: "#10B981", fontSize: 11, fontWeight: "700" },
  h1: { color: "#E9F2FF", fontSize: 22, fontWeight: "800", marginTop: 4 },
  sub: { color: "#A7C0E8", fontSize: 12, lineHeight: 17 },
  countText: { color: "#93C5FD", fontSize: 11, fontWeight: "700", marginTop: 4 },

  // State picker
  stateRow: {
    flexDirection: "row",
    alignItems: "center",
    gap: 6,
    marginTop: 6,
    paddingVertical: 6,
    paddingHorizontal: 10,
    backgroundColor: "rgba(16,185,129,0.08)",
    borderRadius: 8,
    alignSelf: "flex-start",
  },
  stateLabel: { color: "#64748B", fontSize: 11, fontWeight: "600" },
  stateValue: { color: "#10B981", fontSize: 11, fontWeight: "800" },
  stateChevron: { color: "#64748B", fontSize: 9 },
  stateGrid: {
    flexDirection: "row",
    flexWrap: "wrap",
    gap: 6,
    marginTop: 6,
  },
  stateChip: {
    paddingHorizontal: 10,
    paddingVertical: 5,
    borderRadius: 6,
    backgroundColor: "#0F172A",
    borderWidth: 1,
    borderColor: "#1E293B",
  },
  stateChipActive: {
    backgroundColor: "rgba(16,185,129,0.15)",
    borderColor: "#10B981",
  },
  stateChipText: { color: "#64748B", fontSize: 11, fontWeight: "700" },
  stateChipTextActive: { color: "#10B981" },

  // Card
  card: {
    borderWidth: StyleSheet.hairlineWidth,
    borderColor: "#1E293B",
    borderRadius: 14,
    backgroundColor: "#0B1529",
    overflow: "hidden",
  },
  cardInner: { minWidth: 480 },

  // Header row
  headerRow: {
    flexDirection: "row",
    backgroundColor: "#0F172A",
    paddingVertical: 6,
    paddingHorizontal: 8,
    borderBottomWidth: StyleSheet.hairlineWidth,
    borderBottomColor: "#1E293B",
  },
  headerLabel: {
    color: "#475569",
    fontSize: 8,
    fontWeight: "800",
    textTransform: "uppercase",
  },

  // Team row
  teamRow: {
    flexDirection: "row",
    alignItems: "center",
    paddingVertical: 8,
    paddingHorizontal: 8,
    borderBottomWidth: StyleSheet.hairlineWidth,
    borderBottomColor: "rgba(30,41,59,0.5)",
  },

  // Columns
  teamInfoCol: { width: 110, flexDirection: "row", alignItems: "center", gap: 5 },
  teamLogo: { width: 18, height: 18, borderRadius: 9, backgroundColor: "#111827" },
  teamLogoPlaceholder: { width: 18, height: 18, borderRadius: 9, backgroundColor: "#1E293B" },
  teamNameWrap: { flex: 1 },
  teamName: { color: "#E5E7EB", fontSize: 10, fontWeight: "700" },

  statCol: { width: 64, alignItems: "center" },
  statColNarrow: { width: 36, alignItems: "center" },
  statValue: { color: "#CBD5E1", fontSize: 10, fontWeight: "700" },
  statValueMuted: { color: "#475569", fontSize: 10 },

  pitcherCol: { flex: 1, paddingHorizontal: 4 },
  pitcherName: { color: "#93C5FD", fontSize: 10, fontWeight: "700" },

  streakCol: { width: 56, alignItems: "center" },
  streakBadge: {
    borderRadius: 4,
    paddingHorizontal: 5,
    paddingVertical: 2,
  },
  streakText: { fontSize: 8, fontWeight: "800" },

  // Score + bet buttons row
  scoreRow: {
    flexDirection: "row",
    justifyContent: "space-between",
    alignItems: "center",
    paddingHorizontal: 12,
    paddingVertical: 8,
    backgroundColor: "rgba(15,23,42,0.4)",
  },
  scoreLeft: { flexDirection: "row", alignItems: "center", gap: 8 },
  scoreLabel: { color: "#64748B", fontSize: 11, fontWeight: "700" },
  scoreValue: { fontSize: 16, fontWeight: "900" },

  // Bet buttons
  betBtnRow: { flexDirection: "row", gap: 8 },
  betBtn: {
    borderWidth: 1,
    borderRadius: 8,
    paddingHorizontal: 14,
    paddingVertical: 6,
    alignItems: "center",
  },
  nrfiBtn: {
    borderColor: "#10B981",
    backgroundColor: "rgba(16,185,129,0.08)",
  },
  nrfiBtnActive: {
    backgroundColor: "rgba(16,185,129,0.25)",
    borderColor: "#34D399",
  },
  yrfiBtn: {
    borderColor: "#EF4444",
    backgroundColor: "rgba(239,68,68,0.08)",
  },
  yrfiBtnActive: {
    backgroundColor: "rgba(239,68,68,0.25)",
    borderColor: "#F87171",
  },
  betBtnText: { color: "#94A3B8", fontSize: 11, fontWeight: "800" },
  betBtnOdds: { color: "#64748B", fontSize: 9, fontWeight: "700", marginTop: 1 },
  nrfiBtnTextActive: { color: "#A7F3D0" },
  yrfiBtnTextActive: { color: "#FECACA" },

  // Parlay bar
  parlayBar: {
    borderTopWidth: 1,
    borderTopColor: "#10B981",
    backgroundColor: "rgba(2,6,23,0.98)",
    padding: 12,
    gap: 8,
  },
  parlayTopRow: { flexDirection: "row", justifyContent: "space-between", alignItems: "center" },
  parlayTitle: { color: "#E2E8F0", fontSize: 14, fontWeight: "800" },
  parlayClear: { color: "#94A3B8", fontSize: 16, fontWeight: "900", paddingHorizontal: 4 },
  parlayLegs: { gap: 2 },
  parlayLegText: { color: "#C7D2FE", fontSize: 11 },
  parlayBtnRow: { flexDirection: "row", gap: 8 },
  parlayBtn: {
    flex: 1,
    borderWidth: StyleSheet.hairlineWidth,
    borderColor: "#10B981",
    borderRadius: 10,
    backgroundColor: "rgba(16,185,129,0.12)",
    paddingVertical: 10,
    alignItems: "center",
  },
  parlayBtnDisabled: { opacity: 0.4, borderColor: "#334155" },
  parlayBtnText: { color: "#A7F3D0", fontSize: 13, fontWeight: "800" },
  parlayNote: { color: "#64748B", fontSize: 9 },

  // Error / empty
  errorBox: { borderWidth: StyleSheet.hairlineWidth, borderRadius: 12, backgroundColor: "#1F2937", padding: 12 },
  errorTitle: { color: "#FCA5A5", fontWeight: "700" },
  errorText: { color: "#FECACA", marginTop: 4, fontSize: 12 },
  errorRetry: { color: "#E5E7EB", marginTop: 8, fontSize: 12 },
  emptyCard: { borderWidth: StyleSheet.hairlineWidth, borderRadius: 14, backgroundColor: "#0B1529", padding: 16 },
  emptyTitle: { color: "#E5E7EB", fontWeight: "700" },
  emptyText: { color: "#A7C0E8", marginTop: 6, fontSize: 12 },
});
