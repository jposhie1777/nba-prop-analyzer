import { ActivityIndicator, Image, Linking, Pressable, ScrollView, StyleSheet, Text, View } from "react-native";
import { useCallback, useEffect, useMemo, useState } from "react";
import { useRouter } from "expo-router";

import {
  useMlbUpcomingGames,
  useMlbPitchingProps,
  type MlbUpcomingGame,
  type MlbPitchingPropsDetail,
  type KPropPitcher,
  type KPropAltLine,
} from "@/hooks/mlb/useMlbMatchups";
import { useTheme } from "@/store/useTheme";
import { getMlbTeamLogo } from "@/utils/mlbLogos";
import { usePropBetslip } from "@/store/usePropBetslip";
import { useBetslipDrawer } from "@/store/useBetslipDrawer";
import { getBuildPlatform } from "@/utils/parlayBuilder";
import { API_BASE, CLOUD_API_BASE } from "@/lib/config";

// ── Formatters ──────────────────────────────────────────────────────────────

function fmt(value?: number | null, digits = 1): string {
  if (value == null || !Number.isFinite(value)) return "—";
  return Number.isInteger(value) ? `${value}` : value.toFixed(digits);
}

function fmtOdds(value?: number | null): string {
  if (value == null || !Number.isFinite(value) || value === 0) return "—";
  return value > 0 ? `+${Math.round(value)}` : `${Math.round(value)}`;
}

function fmtPct(value?: number | null, digits = 1): string {
  if (value == null || !Number.isFinite(value)) return "—";
  return `${value.toFixed(digits)}%`;
}

// ── Derived analytics ───────────────────────────────────────────────────────

function deriveEdge(projKs: number, line: number) {
  const edge = Math.round((projKs - line) * 10) / 10;
  let grade = "F";
  if (edge >= 2.0) grade = "A+";
  else if (edge >= 1.5) grade = "A";
  else if (edge >= 1.0) grade = "B+";
  else if (edge >= 0.5) grade = "B";
  else if (edge >= 0.0) grade = "C+";
  else if (edge >= -0.5) grade = "C";
  else if (edge >= -1.0) grade = "C-";
  else if (edge >= -1.5) grade = "D";
  return { edge, grade };
}

function gradeTone(grade?: string | null) {
  const g = (grade ?? "").toUpperCase();
  if (g === "A+" || g === "A") return { border: "#10B981", bg: "rgba(16,185,129,0.12)", text: "#A7F3D0" };
  if (g === "B+" || g === "B") return { border: "#22D3EE", bg: "rgba(34,211,238,0.12)", text: "#CFFAFE" };
  if (g === "C+" || g === "C" || g === "C-") return { border: "#F59E0B", bg: "rgba(245,158,11,0.12)", text: "#FDE68A" };
  return { border: "#EF4444", bg: "rgba(239,68,68,0.12)", text: "#FECACA" };
}

function leanColor(lean?: string | null) {
  if (lean === "OVER") return "#10B981";
  if (lean === "UNDER") return "#EF4444";
  return "#94A3B8";
}

// ── Multi-game pitching props fetcher ───────────────────────────────────────

type GamePitchingData = {
  gamePk: number;
  homeTeam: string;
  awayTeam: string;
  startTime: string | null;
  venue: string | null;
  detail: MlbPitchingPropsDetail | null;
  loading: boolean;
  error: string | null;
};

function useMlbAllPitchingProps() {
  const { data: games, loading: gamesLoading, error: gamesError, refetch: refetchGames } = useMlbUpcomingGames();
  const [gameData, setGameData] = useState<GamePitchingData[]>([]);
  const [loading, setLoading] = useState(false);

  const fetchAllGames = useCallback(async (gameList: MlbUpcomingGame[]) => {
    if (!gameList.length) return;
    setLoading(true);

    // Initialize entries
    const initial: GamePitchingData[] = gameList.map((g) => ({
      gamePk: g.game_pk,
      homeTeam: g.home_team ?? "Home",
      awayTeam: g.away_team ?? "Away",
      startTime: g.start_time_utc ?? null,
      venue: g.venue_name ?? null,
      detail: null,
      loading: true,
      error: null,
    }));
    setGameData(initial);

    // Fetch all in parallel
    const bases = [API_BASE, CLOUD_API_BASE].filter(Boolean);
    const results = await Promise.allSettled(
      gameList.map(async (g) => {
        for (const base of bases) {
          try {
            const url = `${base}/mlb/matchups/${g.game_pk}/pitching-props`;
            const resp = await fetch(url, { signal: AbortSignal.timeout(15000) });
            if (resp.ok) return { gamePk: g.game_pk, data: (await resp.json()) as MlbPitchingPropsDetail };
          } catch { /* try next base */ }
        }
        throw new Error("Failed to fetch");
      })
    );

    setGameData((prev) =>
      prev.map((entry, i) => {
        const result = results[i];
        if (result.status === "fulfilled") {
          return { ...entry, detail: result.value.data, loading: false };
        }
        return { ...entry, loading: false, error: "Failed to load" };
      })
    );
    setLoading(false);
  }, []);

  useEffect(() => {
    if (games?.length) fetchAllGames(games);
  }, [games, fetchAllGames]);

  return {
    gameData,
    loading: gamesLoading || loading,
    error: gamesError,
    refetch: refetchGames,
  };
}

// ── Component ───────────────────────────────────────────────────────────────

export function MlbPitchingSummaryScreen() {
  const { gameData, loading, error, refetch } = useMlbAllPitchingProps();
  const { colors } = useTheme();
  const router = useRouter();
  const platform = useMemo(() => getBuildPlatform(), []);
  const addToBetslip = usePropBetslip((s) => s.add);
  const removeFromBetslip = usePropBetslip((s) => s.remove);
  const openBetslip = useBetslipDrawer((s) => s.open);
  const [selectedKeys, setSelectedKeys] = useState<Set<string>>(new Set());
  const [collapsedGames, setCollapsedGames] = useState<Set<number>>(new Set());

  function toggleGameCollapse(gamePk: number) {
    setCollapsedGames((prev) => {
      const next = new Set(prev);
      if (next.has(gamePk)) next.delete(gamePk);
      else next.add(gamePk);
      return next;
    });
  }

  function makeSlipId(pitcher: KPropPitcher, gamePk: number, line?: KPropAltLine | null): string {
    const lineVal = line?.line ?? pitcher.k_line ?? 0;
    return `mlb-k-${gamePk}-${pitcher.pitcher_id}-${lineVal}`;
  }

  function addKPropToSlip(pitcher: KPropPitcher, gamePk: number, line?: KPropAltLine | null) {
    const isAlt = line != null;
    const odds = isAlt ? (line.dk_price ?? line.best_price ?? 0) : (pitcher.dk_price ?? pitcher.k_best_price ?? 0);
    const lineVal = isAlt ? (line.line ?? 0) : (pitcher.k_line ?? 0);
    addToBetslip({
      id: makeSlipId(pitcher, gamePk, line),
      player_id: Number(pitcher.pitcher_id ?? 0),
      player: pitcher.pitcher_name ?? "Pitcher",
      market: `MLB K Over ${lineVal}`,
      side: "over",
      line: lineVal,
      odds: Number(odds),
      matchup: `${pitcher.pitcher_name ?? ""} vs ${pitcher.offense_team ?? ""}`,
      sport: "mlb",
      bookmaker: null,
      dk_event_id: isAlt ? (line.dk_event_id ?? null) : (pitcher.dk_event_id ?? null),
      dk_outcome_code: isAlt ? (line.dk_outcome_code ?? null) : (pitcher.dk_outcome_code ?? null),
      fd_market_id: isAlt ? (line.fd_market_id ?? null) : (pitcher.fd_market_id ?? null),
      fd_selection_id: isAlt ? (line.fd_selection_id ?? null) : (pitcher.fd_selection_id ?? null),
    });
  }

  function togglePitcher(pitcher: KPropPitcher, gamePk: number, line?: KPropAltLine | null) {
    const key = makeSlipId(pitcher, gamePk, line);
    setSelectedKeys((prev) => {
      const next = new Set(prev);
      if (next.has(key)) {
        next.delete(key);
        removeFromBetslip(key);
      } else if (next.size < 10) {
        next.add(key);
        addKPropToSlip(pitcher, gamePk, line);
        openBetslip();
      }
      return next;
    });
  }

  function openUrl(url?: string | null) {
    if (!url) return;
    if (platform === "desktop" && typeof globalThis.open === "function") {
      globalThis.open(url, "_blank");
      return;
    }
    Linking.openURL(url).catch(() => {});
  }

  function getDkUrl(pitcher: KPropPitcher, line?: KPropAltLine | null): string | null {
    if (line) return platform === "desktop" ? (line.dk_desktop ?? null) : (line.dk_ios ?? null);
    return platform === "desktop" ? (pitcher.dk_desktop ?? null) : (pitcher.dk_ios ?? null);
  }

  function getFdUrl(pitcher: KPropPitcher, line?: KPropAltLine | null): string | null {
    if (line) return platform === "desktop" ? (line.fd_desktop ?? null) : (line.fd_ios ?? null);
    return platform === "desktop" ? (pitcher.fd_desktop ?? null) : (pitcher.fd_ios ?? null);
  }

  // Filter to only games that have pitcher data
  const gamesWithData = gameData.filter(
    (g) => g.detail && g.detail.pitchers.length > 0
  );
  const gamesLoading = gameData.filter((g) => g.loading);

  return (
    <ScrollView style={s.screen} contentContainerStyle={s.content}>
      {/* Header */}
      <Text style={s.pageTitle}>Pitching K Props</Text>
      <Text style={s.pageSub}>All games • Strikeout projections & edges</Text>

      {loading ? (
        <View style={s.loadingRow}>
          <ActivityIndicator color="#93C5FD" />
          <Text style={s.loadingText}>Loading pitching props{gamesLoading.length > 0 ? ` (${gamesLoading.length} games)` : ""}...</Text>
        </View>
      ) : null}

      {error ? (
        <Pressable onPress={refetch} style={s.errorBox}>
          <Text style={s.errorTitle}>Failed to load games.</Text>
          <Text style={s.errorText}>{error}</Text>
          <Text style={s.errorRetry}>Tap to retry</Text>
        </Pressable>
      ) : null}

      {!loading && !error && gamesWithData.length === 0 ? (
        <View style={s.emptyBox}>
          <Text style={s.emptyTitle}>No pitching prop data available yet.</Text>
          <Text style={s.emptySub}>Once lines are posted, this view will auto-populate.</Text>
        </View>
      ) : null}

      {/* Game panels */}
      {gamesWithData.map((game) => {
        const isCollapsed = collapsedGames.has(game.gamePk);
        const pitchers = game.detail?.pitchers ?? [];
        const homeLogo = getMlbTeamLogo(game.homeTeam);
        const awayLogo = getMlbTeamLogo(game.awayTeam);
        const startStr = game.startTime
          ? new Date(game.startTime).toLocaleTimeString([], { hour: "numeric", minute: "2-digit" })
          : "TBD";

        return (
          <View key={game.gamePk} style={s.gamePanel}>
            {/* Game header - tap to collapse */}
            <Pressable onPress={() => toggleGameCollapse(game.gamePk)} style={s.gameHeader}>
              <View style={s.gameTeams}>
                {awayLogo ? <Image source={{ uri: awayLogo }} style={s.teamLogo} /> : null}
                <Text style={s.gameTeamText}>{game.awayTeam}</Text>
                <Text style={s.atText}>@</Text>
                <Text style={s.gameTeamText}>{game.homeTeam}</Text>
                {homeLogo ? <Image source={{ uri: homeLogo }} style={s.teamLogo} /> : null}
              </View>
              <View style={s.gameHeaderRight}>
                <Text style={s.gameTime}>{startStr}</Text>
                <Text style={s.chevron}>{isCollapsed ? "▼" : "▲"}</Text>
              </View>
            </Pressable>

            {/* Pitcher cards */}
            {!isCollapsed ? (
              <View style={s.pitcherList}>
                {pitchers.map((pitcher) => (
                  <PitcherSummaryCard
                    key={String(pitcher.pitcher_id)}
                    pitcher={pitcher}
                    gamePk={game.gamePk}
                    selectedKeys={selectedKeys}
                    togglePitcher={togglePitcher}
                    addKPropToSlip={addKPropToSlip}
                    openUrl={openUrl}
                    getDkUrl={getDkUrl}
                    getFdUrl={getFdUrl}
                    openBetslip={openBetslip}
                    router={router}
                    homeTeam={game.homeTeam}
                    awayTeam={game.awayTeam}
                  />
                ))}
              </View>
            ) : null}
          </View>
        );
      })}
    </ScrollView>
  );
}

// ── Pitcher Summary Card ────────────────────────────────────────────────────

function PitcherSummaryCard({
  pitcher,
  gamePk,
  selectedKeys,
  togglePitcher,
  addKPropToSlip,
  openUrl,
  getDkUrl,
  getFdUrl,
  openBetslip,
  router,
  homeTeam,
  awayTeam,
}: {
  pitcher: KPropPitcher;
  gamePk: number;
  selectedKeys: Set<string>;
  togglePitcher: (p: KPropPitcher, gp: number, l?: KPropAltLine | null) => void;
  addKPropToSlip: (p: KPropPitcher, gp: number, l?: KPropAltLine | null) => void;
  openUrl: (url?: string | null) => void;
  getDkUrl: (p: KPropPitcher, l?: KPropAltLine | null) => string | null;
  getFdUrl: (p: KPropPitcher, l?: KPropAltLine | null) => string | null;
  openBetslip: () => void;
  router: any;
  homeTeam: string;
  awayTeam: string;
}) {
  const projKs = pitcher.proj_ks ?? 0;
  const line = pitcher.k_line ?? 0;
  const derived = deriveEdge(projKs, line);
  const tone = gradeTone(derived.grade);
  const standardKey = `mlb-k-${gamePk}-${pitcher.pitcher_id}-${line}`;
  const isSelected = selectedKeys.has(standardKey);

  return (
    <View style={s.pitcherCard}>
      {/* Header row */}
      <View style={s.pitcherHeader}>
        <View style={{ flex: 1 }}>
          <Text style={s.pitcherName}>{pitcher.pitcher_name ?? "Pitcher"}</Text>
          <Text style={s.pitcherSub}>
            {pitcher.pitcher_hand ?? "RHP"} • {pitcher.team_code ?? ""} vs {pitcher.opp_team_code ?? ""}
          </Text>
        </View>
        <View style={[s.gradePill, { borderColor: tone.border, backgroundColor: tone.bg }]}>
          <Text style={[s.gradeText, { color: tone.text }]}>{derived.grade}</Text>
        </View>
      </View>

      {/* Stats row */}
      <View style={s.statsRow}>
        <View style={s.statBox}>
          <Text style={s.statLabel}>LINE</Text>
          <Text style={s.statVal}>{fmt(line, 1)}</Text>
        </View>
        <View style={s.statBox}>
          <Text style={s.statLabel}>PROJ</Text>
          <Text style={[s.statVal, { color: "#22D3EE" }]}>{fmt(projKs, 1)}</Text>
        </View>
        <View style={s.statBox}>
          <Text style={s.statLabel}>EDGE</Text>
          <Text style={[s.statVal, { color: derived.edge >= 0.5 ? "#10B981" : derived.edge <= -0.5 ? "#EF4444" : "#94A3B8" }]}>
            {derived.edge > 0 ? `+${derived.edge}` : `${derived.edge}`}
          </Text>
        </View>
        <View style={s.statBox}>
          <Text style={s.statLabel}>K/9</Text>
          <Text style={s.statVal}>{fmt(pitcher.strikeouts_per_9, 2)}</Text>
        </View>
        <View style={s.statBox}>
          <Text style={s.statLabel}>K%</Text>
          <Text style={s.statVal}>{fmtPct(pitcher.k_pct)}</Text>
        </View>
        <View style={s.statBox}>
          <Text style={s.statLabel}>WHIP</Text>
          <Text style={s.statVal}>{fmt(pitcher.whip, 2)}</Text>
        </View>
      </View>

      {/* Price + Bet row */}
      <View style={s.priceRow}>
        <Text style={s.priceText}>
          O {fmt(line, 1)} Ks • {fmtOdds(pitcher.k_best_price)} • {pitcher.k_best_book ?? "—"}
        </Text>
        <View style={s.betBtns}>
          <Pressable style={s.selectBtn} onPress={() => togglePitcher(pitcher, gamePk)}>
            <Text style={s.selectBtnText}>{isSelected ? "☑" : "☐"}</Text>
          </Pressable>
          <Pressable
            style={[s.bookBtn, !getDkUrl(pitcher) ? s.bookBtnDisabled : null]}
            disabled={!getDkUrl(pitcher)}
            onPress={() => {
              addKPropToSlip(pitcher, gamePk);
              openBetslip();
              openUrl(getDkUrl(pitcher));
            }}
          >
            <Text style={s.bookBtnText}>DK</Text>
          </Pressable>
          <Pressable
            style={[s.bookBtn, !getFdUrl(pitcher) ? s.bookBtnDisabled : null]}
            disabled={!getFdUrl(pitcher)}
            onPress={() => {
              addKPropToSlip(pitcher, gamePk);
              openBetslip();
              openUrl(getFdUrl(pitcher));
            }}
          >
            <Text style={s.bookBtnText}>FD</Text>
          </Pressable>
        </View>
      </View>

      {/* Hit rates row */}
      <View style={s.hitRateRow}>
        <Text style={s.hitRateItem}>L10: {pitcher.hit_rate_l10 ?? "—"}</Text>
        <Text style={s.hitRateSep}>•</Text>
        <Text style={s.hitRateItem}>Szn: {pitcher.hit_rate_season ?? "—"}</Text>
        <Text style={s.hitRateSep}>•</Text>
        <Text style={s.hitRateItem}>Avg L10: {fmt(pitcher.avg_l10, 1)}</Text>
        <Text style={s.hitRateSep}>•</Text>
        <Text style={s.hitRateItem}>Streak: {pitcher.streak ?? "—"}</Text>
      </View>

      {/* View full detail link */}
      <Pressable
        onPress={() =>
          router.push({
            pathname: "/(tabs)/mlb/pitching-props/[gamePk]" as any,
            params: { gamePk: String(gamePk), homeTeam, awayTeam },
          })
        }
        style={s.detailLink}
      >
        <Text style={s.detailLinkText}>View Full Analysis →</Text>
      </Pressable>
    </View>
  );
}

// ── Styles ───────────────────────────────────────────────────────────────────

const s = StyleSheet.create({
  screen: { flex: 1, backgroundColor: "#050A18" },
  content: { padding: 12, gap: 10, paddingBottom: 40 },

  // Page header
  pageTitle: { color: "#E5E7EB", fontSize: 18, fontWeight: "800" },
  pageSub: { color: "#64748B", fontSize: 12, marginBottom: 4 },

  // Loading
  loadingRow: { flexDirection: "row", alignItems: "center", gap: 8, paddingVertical: 16 },
  loadingText: { color: "#94A3B8", fontSize: 13 },

  // Error
  errorBox: { borderWidth: StyleSheet.hairlineWidth, borderColor: "#374151", borderRadius: 12, padding: 16, gap: 6, backgroundColor: "#0B1529" },
  errorTitle: { color: "#F87171", fontSize: 14, fontWeight: "800" },
  errorText: { color: "#94A3B8", fontSize: 12 },
  errorRetry: { color: "#93C5FD", fontSize: 12, fontWeight: "700" },

  // Empty
  emptyBox: { borderWidth: StyleSheet.hairlineWidth, borderColor: "#374151", borderRadius: 12, padding: 20, gap: 6, backgroundColor: "#0B1529", alignItems: "center" },
  emptyTitle: { color: "#E5E7EB", fontSize: 15, fontWeight: "700" },
  emptySub: { color: "#94A3B8", fontSize: 12 },

  // Game panel
  gamePanel: { borderWidth: StyleSheet.hairlineWidth, borderColor: "#1E293B", borderRadius: 12, backgroundColor: "#0B1529", overflow: "hidden" },
  gameHeader: { flexDirection: "row", alignItems: "center", justifyContent: "space-between", paddingHorizontal: 12, paddingVertical: 10, backgroundColor: "#111827" },
  gameTeams: { flexDirection: "row", alignItems: "center", gap: 6, flex: 1 },
  teamLogo: { width: 22, height: 22, borderRadius: 11 },
  gameTeamText: { color: "#E5E7EB", fontSize: 13, fontWeight: "800" },
  atText: { color: "#64748B", fontSize: 11 },
  gameHeaderRight: { flexDirection: "row", alignItems: "center", gap: 8 },
  gameTime: { color: "#94A3B8", fontSize: 12, fontWeight: "700" },
  chevron: { color: "#64748B", fontSize: 10 },

  // Pitcher list
  pitcherList: { gap: 0 },

  // Pitcher card
  pitcherCard: { paddingHorizontal: 12, paddingVertical: 10, borderTopWidth: StyleSheet.hairlineWidth, borderTopColor: "#1E293B", gap: 8 },

  // Pitcher header
  pitcherHeader: { flexDirection: "row", alignItems: "center", justifyContent: "space-between", gap: 8 },
  pitcherName: { color: "#E5E7EB", fontSize: 15, fontWeight: "800" },
  pitcherSub: { color: "#94A3B8", fontSize: 11, marginTop: 1 },
  gradePill: { borderWidth: 1, borderRadius: 999, paddingHorizontal: 10, paddingVertical: 4 },
  gradeText: { fontSize: 12, fontWeight: "800" },

  // Stats row
  statsRow: { flexDirection: "row", backgroundColor: "#0F172A", borderRadius: 8, padding: 8 },
  statBox: { flex: 1, alignItems: "center", gap: 2 },
  statLabel: { color: "#64748B", fontSize: 8, fontWeight: "700" },
  statVal: { color: "#E5E7EB", fontSize: 13, fontWeight: "700" },

  // Price row
  priceRow: { flexDirection: "row", alignItems: "center", justifyContent: "space-between" },
  priceText: { color: "#BFDBFE", fontSize: 11, fontWeight: "700", flex: 1 },
  betBtns: { flexDirection: "row", gap: 6, alignItems: "center" },
  selectBtn: { paddingHorizontal: 6, paddingVertical: 4 },
  selectBtnText: { color: "#93C5FD", fontSize: 14 },
  bookBtn: { borderRadius: 6, paddingHorizontal: 10, paddingVertical: 5, backgroundColor: "#1E3A5F" },
  bookBtnDisabled: { opacity: 0.35 },
  bookBtnText: { color: "#E5E7EB", fontSize: 11, fontWeight: "700" },

  // Hit rates
  hitRateRow: { flexDirection: "row", flexWrap: "wrap", alignItems: "center", gap: 2 },
  hitRateItem: { color: "#94A3B8", fontSize: 10, fontWeight: "600" },
  hitRateSep: { color: "#475569", fontSize: 10 },

  // Detail link
  detailLink: { alignSelf: "flex-end" },
  detailLinkText: { color: "#22D3EE", fontSize: 11, fontWeight: "700" },
});
