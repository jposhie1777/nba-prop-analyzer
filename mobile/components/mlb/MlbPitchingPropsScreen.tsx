import { ActivityIndicator, Image, Linking, Pressable, ScrollView, StyleSheet, Text, View } from "react-native";
import { useLocalSearchParams, useRouter } from "expo-router";
import { useMemo, useState } from "react";

import { useMlbPitchingProps, type KPropPitcher, type KPropAltLine } from "@/hooks/mlb/useMlbMatchups";
import { useTheme } from "@/store/useTheme";
import { getMlbTeamLogo } from "@/utils/mlbLogos";
import { usePropBetslip } from "@/store/usePropBetslip";
import { useBetslipDrawer } from "@/store/useBetslipDrawer";
import { getBuildPlatform } from "@/utils/parlayBuilder";

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

// ── Derived analytics for a chosen line ─────────────────────────────────────

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

  let lean = "PASS";
  if (edge >= 0.5) lean = "OVER";
  else if (edge <= -0.5) lean = "UNDER";

  let confidence = "LOW";
  if (Math.abs(edge) >= 1.0) confidence = "HIGH";
  else if (Math.abs(edge) >= 0.5) confidence = "MEDIUM";

  return { edge, grade, lean, confidence };
}

// ── Grade styling ───────────────────────────────────────────────────────────

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

function confidenceColor(conf?: string | null) {
  if (conf === "HIGH") return "#10B981";
  if (conf === "MEDIUM") return "#F59E0B";
  return "#64748B";
}

// ── Collapsible card ────────────────────────────────────────────────────────

function CollapsibleCard({ title, defaultOpen = false, children }: { title: string; defaultOpen?: boolean; children: React.ReactNode }) {
  const [open, setOpen] = useState(defaultOpen);
  return (
    <View style={cs.card}>
      <Pressable onPress={() => setOpen(!open)} style={cs.cardHeader}>
        <Text style={cs.cardTitle}>{title}</Text>
        <Text style={cs.cardChevron}>{open ? "▲" : "▼"}</Text>
      </Pressable>
      {open ? <View style={cs.cardBody}>{children}</View> : null}
    </View>
  );
}

const cs = StyleSheet.create({
  card: { borderWidth: StyleSheet.hairlineWidth, borderColor: "#374151", borderRadius: 10, backgroundColor: "#111827", overflow: "hidden" },
  cardHeader: { flexDirection: "row", alignItems: "center", justifyContent: "space-between", paddingHorizontal: 10, paddingVertical: 9 },
  cardTitle: { color: "#94A3B8", fontSize: 11, fontWeight: "800" },
  cardChevron: { color: "#64748B", fontSize: 10 },
  cardBody: { paddingHorizontal: 10, paddingBottom: 10 },
});

// ── Component ───────────────────────────────────────────────────────────────

export function MlbPitchingPropsScreen() {
  const { gamePk: rawGamePk, homeTeam, awayTeam } = useLocalSearchParams<{
    gamePk: string;
    homeTeam?: string;
    awayTeam?: string;
  }>();
  const gamePk = rawGamePk ? Number(rawGamePk) : null;
  const { data, loading, error, refetch } = useMlbPitchingProps(gamePk);
  const { colors } = useTheme();
  const router = useRouter();
  const platform = useMemo(() => getBuildPlatform(), []);
  const addToBetslip = usePropBetslip((s) => s.add);
  const removeFromBetslip = usePropBetslip((s) => s.remove);
  const openBetslip = useBetslipDrawer((s) => s.open);
  const [selectedKeys, setSelectedKeys] = useState<Set<string>>(new Set());
  // Line selector state: pitcher_id -> chosen line value (null = standard)
  const [chosenLines, setChosenLines] = useState<Record<number, number | null>>({});

  const game = data?.game;
  const homeTeamName = game?.home_team ?? homeTeam ?? "Home";
  const awayTeamName = game?.away_team ?? awayTeam ?? "Away";
  const homeLogo = getMlbTeamLogo(homeTeamName);
  const awayLogo = getMlbTeamLogo(awayTeamName);

  function makeSlipId(pitcher: KPropPitcher, line?: KPropAltLine | null): string {
    const lineVal = line?.line ?? pitcher.k_line ?? 0;
    return `mlb-k-${gamePk}-${pitcher.pitcher_id}-${lineVal}`;
  }

  function addKPropToSlip(pitcher: KPropPitcher, line?: KPropAltLine | null) {
    const isAlt = line != null;
    const odds = isAlt ? (line.dk_price ?? line.best_price ?? 0) : (pitcher.dk_price ?? pitcher.k_best_price ?? 0);
    const lineVal = isAlt ? (line.line ?? 0) : (pitcher.k_line ?? 0);
    addToBetslip({
      id: makeSlipId(pitcher, line),
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

  function togglePitcher(pitcher: KPropPitcher, line?: KPropAltLine | null) {
    const key = makeSlipId(pitcher, line);
    setSelectedKeys((prev) => {
      const next = new Set(prev);
      if (next.has(key)) {
        next.delete(key);
        removeFromBetslip(key);
      } else if (next.size < 10) {
        next.add(key);
        addKPropToSlip(pitcher, line);
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

  return (
    <ScrollView style={s.screen} contentContainerStyle={s.content}>
      {/* ── Navigation ── */}
      <View style={s.navRow}>
        <Pressable onPress={() => router.push("/(tabs)/mlb" as any)} style={s.navBtn}>
          <Text style={s.navBtnText}>← MLB</Text>
        </Pressable>
        <Pressable onPress={() => router.push("/(tabs)/home")} style={s.navBtn}>
          <Text style={s.navBtnText}>Home</Text>
        </Pressable>
      </View>

      {/* ── Sub-tab indicator ── */}
      <View style={s.tabRow}>
        <Pressable
          onPress={() =>
            router.push({
              pathname: "/(tabs)/mlb/hr-matchup/[gamePk]" as any,
              params: { gamePk: String(gamePk), homeTeam: homeTeamName, awayTeam: awayTeamName },
            })
          }
          style={s.tabInactive}
        >
          <Text style={s.tabTextInactive}>HR Matchup</Text>
        </Pressable>
        <View style={s.tabActive}>
          <Text style={s.tabTextActive}>Pitching</Text>
        </View>
        <Pressable
          onPress={() =>
            router.push({
              pathname: "/(tabs)/mlb/lineup-matchup/[gamePk]" as any,
              params: { gamePk: String(gamePk), homeTeam: homeTeamName, awayTeam: awayTeamName },
            })
          }
          style={s.tabInactive}
        >
          <Text style={s.tabTextInactive}>Lineup</Text>
        </Pressable>
      </View>

      {/* ── Hero / Matchup Header ── */}
      <View style={[s.hero, { borderColor: colors.border.subtle }]}>
        <Text style={s.eyebrow}>MLB PITCHING PROPS</Text>
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
              ☁ {game?.weather?.weather_indicator ?? "Weather"}{" "}
              {game?.weather?.game_temp != null ? `${Math.round(game.weather.game_temp)}°` : ""}
              {game?.weather?.wind_speed != null
                ? ` • ${Math.round(game.weather.wind_speed)} mph ${game?.weather?.wind_direction_label ?? ""}`
                : ""}
            </Text>
          </View>
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
          <Text style={s.errorTitle}>Failed to load pitching props.</Text>
          <Text style={s.errorText}>{error}</Text>
          <Text style={s.errorRetry}>Tap to retry</Text>
        </Pressable>
      ) : null}

      {/* ── Pitcher Cards ── */}
      {(data?.pitchers ?? []).map((pitcher) => (
        <PitcherCard
          key={String(pitcher.pitcher_id)}
          pitcher={pitcher}
          gamePk={gamePk}
          selectedKeys={selectedKeys}
          chosenLine={chosenLines[pitcher.pitcher_id ?? 0] ?? null}
          setChosenLine={(line) =>
            setChosenLines((prev) => ({ ...prev, [pitcher.pitcher_id ?? 0]: line }))
          }
          togglePitcher={togglePitcher}
          addKPropToSlip={addKPropToSlip}
          openUrl={openUrl}
          getDkUrl={getDkUrl}
          getFdUrl={getFdUrl}
          openBetslip={openBetslip}
          colors={colors}
        />
      ))}

      {!loading && !error && !(data?.pitchers?.length) ? (
        <View style={[s.panel, { borderColor: colors.border.subtle }]}>
          <Text style={s.emptyTitle}>No pitching prop data for this game yet.</Text>
          <Text style={s.emptySub}>Once ingest runs, this view will auto-populate.</Text>
        </View>
      ) : null}
    </ScrollView>
  );
}

// ── Pitcher Card ────────────────────────────────────────────────────────────

function PitcherCard({
  pitcher,
  gamePk,
  selectedKeys,
  chosenLine,
  setChosenLine,
  togglePitcher,
  addKPropToSlip,
  openUrl,
  getDkUrl,
  getFdUrl,
  openBetslip,
  colors,
}: {
  pitcher: KPropPitcher;
  gamePk: number | null;
  selectedKeys: Set<string>;
  chosenLine: number | null;
  setChosenLine: (line: number | null) => void;
  togglePitcher: (p: KPropPitcher, l?: KPropAltLine | null) => void;
  addKPropToSlip: (p: KPropPitcher, l?: KPropAltLine | null) => void;
  openUrl: (url?: string | null) => void;
  getDkUrl: (p: KPropPitcher, l?: KPropAltLine | null) => string | null;
  getFdUrl: (p: KPropPitcher, l?: KPropAltLine | null) => string | null;
  openBetslip: () => void;
  colors: any;
}) {
  const projKs = pitcher.proj_ks ?? 0;
  const standardLine = pitcher.k_line ?? 0;

  // Build all available lines for the selector
  const allLines = useMemo(() => {
    const lines: { value: number; label: string; isStandard: boolean; altData?: KPropAltLine }[] = [
      { value: standardLine, label: `${standardLine} (Std)`, isStandard: true },
    ];
    for (const alt of pitcher.alt_lines ?? []) {
      if (alt.line != null && alt.line !== standardLine) {
        lines.push({ value: alt.line, label: `${alt.line}`, isStandard: false, altData: alt });
      }
    }
    lines.sort((a, b) => a.value - b.value);
    return lines;
  }, [pitcher.alt_lines, standardLine]);

  // Active line for analytics display
  const activeLine = chosenLine ?? standardLine;
  const derived = deriveEdge(projKs, activeLine);
  const tone = gradeTone(derived.grade);

  // Find alt data for the active line (for bet buttons)
  const activeAltData = useMemo(() => {
    if (activeLine === standardLine) return null;
    return (pitcher.alt_lines ?? []).find((a) => a.line === activeLine) ?? null;
  }, [activeLine, standardLine, pitcher.alt_lines]);

  const activePrice = activeAltData ? (activeAltData.best_price ?? 0) : (pitcher.k_best_price ?? 0);
  const activeBook = activeAltData ? (activeAltData.best_book ?? "") : (pitcher.k_best_book ?? "");

  const standardKey = `mlb-k-${gamePk}-${pitcher.pitcher_id}-${standardLine}`;
  const isStandardSelected = selectedKeys.has(standardKey);

  const oppK = pitcher.opp_team_k;
  const oppSeason = oppK?.splits?.["Season"];
  const oppL15 = oppK?.splits?.["L15 Days"];
  const oppVsHand = pitcher.pitcher_hand === "LHP"
    ? oppK?.splits?.["vs LHP"]
    : oppK?.splits?.["vs RHP"];

  return (
    <View style={[s.panel, { borderColor: colors.border.subtle }]}>
      {/* Pitcher header */}
      <View style={s.pitcherHeader}>
        <View style={{ flex: 1 }}>
          <Text style={s.pitcherName}>{pitcher.pitcher_name ?? "Pitcher"}</Text>
          <Text style={s.pitcherSub}>
            {pitcher.pitcher_hand ?? "RHP"} • {pitcher.team_code ?? ""} vs {pitcher.opp_team_code ?? ""}
          </Text>
        </View>
        <View style={[s.gradePill, { borderColor: tone.border, backgroundColor: tone.bg }]}>
          <Text style={[s.gradeText, { color: tone.text }]}>
            {derived.grade} • {derived.lean}
          </Text>
        </View>
      </View>

      {/* ── Line Selector ── */}
      <View style={s.lineSelectorWrap}>
        <Text style={s.lineSelectorLabel}>Analyze Line:</Text>
        <ScrollView horizontal showsHorizontalScrollIndicator={false} contentContainerStyle={s.lineSelectorRow}>
          {allLines.map((l) => {
            const isActive = l.value === activeLine;
            return (
              <Pressable
                key={`ls-${l.value}`}
                onPress={() => setChosenLine(l.isStandard ? null : l.value)}
                style={[s.lineChip, isActive ? s.lineChipActive : null]}
              >
                <Text style={[s.lineChipText, isActive ? s.lineChipTextActive : null]}>
                  {l.label}
                </Text>
              </Pressable>
            );
          })}
        </ScrollView>
      </View>

      {/* Projection + Line (reactive to selector) */}
      <View style={s.projRow}>
        <View style={s.projBox}>
          <Text style={s.projLabel}>LINE</Text>
          <Text style={s.projValue}>{fmt(activeLine, 1)}</Text>
        </View>
        <View style={s.projBox}>
          <Text style={s.projLabel}>PROJ Ks</Text>
          <Text style={[s.projValue, { color: "#22D3EE" }]}>{fmt(projKs, 1)}</Text>
        </View>
        <View style={s.projBox}>
          <Text style={s.projLabel}>EDGE</Text>
          <Text style={[s.projValue, { color: leanColor(derived.lean) }]}>
            {derived.edge > 0 ? `+${derived.edge}` : `${derived.edge}`}
          </Text>
        </View>
        <View style={s.projBox}>
          <Text style={s.projLabel}>GRADE</Text>
          <Text style={[s.projValue, { color: tone.text }]}>{derived.grade}</Text>
        </View>
        <View style={s.projBox}>
          <Text style={s.projLabel}>CONF</Text>
          <Text style={[s.projValue, { color: confidenceColor(derived.confidence) }]}>
            {derived.confidence}
          </Text>
        </View>
      </View>

      {/* Price for active line */}
      <View style={s.activePriceRow}>
        <Text style={s.activePriceText}>
          Over {fmt(activeLine, 1)} • {fmtOdds(activePrice)} • {activeBook}
        </Text>
      </View>

      {/* ── Collapsible Analytics Sections ── */}

      <CollapsibleCard title="Pitcher K Profile">
        <View style={s.statsGrid}>
          <View style={s.statItem}><Text style={s.statLabel}>K/9</Text><Text style={s.statVal}>{fmt(pitcher.strikeouts_per_9, 2)}</Text></View>
          <View style={s.statItem}><Text style={s.statLabel}>K%</Text><Text style={s.statVal}>{fmtPct(pitcher.k_pct)}</Text></View>
          <View style={s.statItem}><Text style={s.statLabel}>K:BB</Text><Text style={s.statVal}>{fmt(pitcher.strikeout_walk_ratio, 2)}</Text></View>
          <View style={s.statItem}><Text style={s.statLabel}>Strike%</Text><Text style={s.statVal}>{fmtPct(pitcher.strike_pct)}</Text></View>
          <View style={s.statItem}><Text style={s.statLabel}>WHIP</Text><Text style={s.statVal}>{fmt(pitcher.whip, 2)}</Text></View>
          <View style={s.statItem}><Text style={s.statLabel}>IP</Text><Text style={s.statVal}>{fmt(pitcher.ip, 1)}</Text></View>
          <View style={s.statItem}><Text style={s.statLabel}>Proj IP</Text><Text style={s.statVal}>{fmt(pitcher.proj_ip, 1)}</Text></View>
          <View style={s.statItem}><Text style={s.statLabel}>BF</Text><Text style={s.statVal}>{fmt(pitcher.batters_faced, 0)}</Text></View>
        </View>
      </CollapsibleCard>

      <CollapsibleCard title="Arsenal">
        <View style={s.statsGrid}>
          <View style={s.statItem}><Text style={s.statLabel}>Whiff%</Text><Text style={s.statVal}>{fmtPct((pitcher.arsenal_whiff_rate ?? 0) * 100)}</Text></View>
          <View style={s.statItem}><Text style={s.statLabel}>Arsenal K%</Text><Text style={s.statVal}>{fmtPct((pitcher.arsenal_k_pct ?? 0) * 100)}</Text></View>
          <View style={s.statItem}><Text style={s.statLabel}>Best Whiff</Text><Text style={s.statVal}>{fmtPct((pitcher.max_pitch_whiff ?? 0) * 100)}</Text></View>
          <View style={s.statItem}><Text style={s.statLabel}>Pitches</Text><Text style={s.statVal}>{fmt(pitcher.pitch_type_count, 0)}</Text></View>
        </View>
      </CollapsibleCard>

      {pitcher.hand_split ? (
        <CollapsibleCard title={`Split: ${pitcher.hand_split}`}>
          <View style={s.statsGrid}>
            <View style={s.statItem}><Text style={s.statLabel}>K/9</Text><Text style={s.statVal}>{fmt(pitcher.hand_k_per_9, 2)}</Text></View>
            <View style={s.statItem}><Text style={s.statLabel}>K%</Text><Text style={s.statVal}>{fmtPct(pitcher.hand_k_pct)}</Text></View>
          </View>
        </CollapsibleCard>
      ) : null}

      {oppK?.team_name ? (
        <CollapsibleCard title={`Opp K Vulnerability: ${oppK.team_name}`}>
          <View style={s.statsGrid}>
            <View style={s.statItem}>
              <Text style={s.statLabel}>Season Rank</Text>
              <Text style={s.statVal}>#{oppSeason?.rank ?? "—"}</Text>
            </View>
            <View style={s.statItem}>
              <Text style={s.statLabel}>Season Ks</Text>
              <Text style={s.statVal}>{oppSeason?.value ?? "—"}</Text>
            </View>
            <View style={s.statItem}>
              <Text style={s.statLabel}>L15 Rank</Text>
              <Text style={s.statVal}>#{oppL15?.rank ?? "—"}</Text>
            </View>
            <View style={s.statItem}>
              <Text style={s.statLabel}>L15 Ks</Text>
              <Text style={s.statVal}>{oppL15?.value ?? "—"}</Text>
            </View>
            {oppVsHand ? (
              <>
                <View style={s.statItem}>
                  <Text style={s.statLabel}>vs {pitcher.pitcher_hand} Rank</Text>
                  <Text style={s.statVal}>#{oppVsHand.rank ?? "—"}</Text>
                </View>
                <View style={s.statItem}>
                  <Text style={s.statLabel}>vs {pitcher.pitcher_hand} Ks</Text>
                  <Text style={s.statVal}>{oppVsHand.value ?? "—"}</Text>
                </View>
              </>
            ) : null}
            <View style={s.statItem}>
              <Text style={s.statLabel}>K Adj</Text>
              <Text style={[s.statVal, { color: (pitcher.team_k_adj ?? 1) > 1 ? "#10B981" : "#EF4444" }]}>
                {fmt(pitcher.team_k_adj, 3)}x
              </Text>
            </View>
          </View>
        </CollapsibleCard>
      ) : null}

      <CollapsibleCard title="Hit Rates & Averages">
        <View style={s.statsGrid}>
          <View style={s.statItem}><Text style={s.statLabel}>L10</Text><Text style={s.statVal}>{pitcher.hit_rate_l10 ?? "—"}</Text></View>
          <View style={s.statItem}><Text style={s.statLabel}>Season</Text><Text style={s.statVal}>{pitcher.hit_rate_season ?? "—"}</Text></View>
          <View style={s.statItem}><Text style={s.statLabel}>vs Team</Text><Text style={s.statVal}>{pitcher.hit_rate_vs_team ?? "—"}</Text></View>
          <View style={s.statItem}><Text style={s.statLabel}>Avg L10</Text><Text style={s.statVal}>{fmt(pitcher.avg_l10, 1)}</Text></View>
          <View style={s.statItem}><Text style={s.statLabel}>Avg H/A</Text><Text style={s.statVal}>{fmt(pitcher.avg_home_away, 1)}</Text></View>
          <View style={s.statItem}><Text style={s.statLabel}>Avg vs Opp</Text><Text style={s.statVal}>{fmt(pitcher.avg_vs_opponent, 1)}</Text></View>
          <View style={s.statItem}><Text style={s.statLabel}>Streak</Text><Text style={s.statVal}>{pitcher.streak ?? "—"}</Text></View>
          <View style={s.statItem}><Text style={s.statLabel}>PF Rating</Text><Text style={s.statVal}>{fmt(pitcher.pf_rating, 1)}</Text></View>
        </View>
      </CollapsibleCard>

      {/* ── Bet buttons (for standard line — betslip is separate from line selector) ── */}
      <View style={s.betSection}>
        <Text style={s.betLabel}>
          Bet: Over {fmt(pitcher.k_line, 1)} Ks {fmtOdds(pitcher.k_best_price)} ({pitcher.k_best_book ?? "—"})
        </Text>
        <View style={s.betRow}>
          <Pressable style={s.selectBtn} onPress={() => togglePitcher(pitcher)}>
            <Text style={s.selectBtnText}>{isStandardSelected ? "☑ Selected" : "☐ Select for Parlay"}</Text>
          </Pressable>
        </View>
        <View style={s.betRow}>
          <Pressable
            style={[s.bookBtn, !getDkUrl(pitcher) ? s.bookBtnDisabled : null]}
            disabled={!getDkUrl(pitcher)}
            onPress={() => {
              addKPropToSlip(pitcher);
              openBetslip();
              openUrl(getDkUrl(pitcher));
            }}
          >
            <Text style={s.bookBtnText}>Bet DraftKings {fmtOdds(pitcher.dk_price)}</Text>
          </Pressable>
          <Pressable
            style={[s.bookBtn, !getFdUrl(pitcher) ? s.bookBtnDisabled : null]}
            disabled={!getFdUrl(pitcher)}
            onPress={() => {
              addKPropToSlip(pitcher);
              openBetslip();
              openUrl(getFdUrl(pitcher));
            }}
          >
            <Text style={s.bookBtnText}>Bet FanDuel {fmtOdds(pitcher.fd_price)}</Text>
          </Pressable>
        </View>
      </View>

      {/* Alt lines (betslip actions) */}
      {(pitcher.alt_lines ?? []).length > 0 ? (
        <AltLinesSection
          pitcher={pitcher}
          gamePk={gamePk}
          selectedKeys={selectedKeys}
          togglePitcher={togglePitcher}
          openUrl={openUrl}
          getDkUrl={getDkUrl}
          getFdUrl={getFdUrl}
          addKPropToSlip={addKPropToSlip}
          openBetslip={openBetslip}
        />
      ) : null}
    </View>
  );
}

// ── Alt Lines collapsible section ───────────────────────────────────────────

function AltLinesSection({
  pitcher,
  gamePk,
  selectedKeys,
  togglePitcher,
  openUrl,
  getDkUrl,
  getFdUrl,
  addKPropToSlip,
  openBetslip,
}: {
  pitcher: KPropPitcher;
  gamePk: number | null;
  selectedKeys: Set<string>;
  togglePitcher: (p: KPropPitcher, l?: KPropAltLine | null) => void;
  openUrl: (url?: string | null) => void;
  getDkUrl: (p: KPropPitcher, l?: KPropAltLine | null) => string | null;
  getFdUrl: (p: KPropPitcher, l?: KPropAltLine | null) => string | null;
  addKPropToSlip: (p: KPropPitcher, l?: KPropAltLine | null) => void;
  openBetslip: () => void;
}) {
  const [expanded, setExpanded] = useState(false);
  const projKs = pitcher.proj_ks ?? 0;

  return (
    <View style={s.altSection}>
      <Pressable onPress={() => setExpanded(!expanded)} style={s.altToggle}>
        <Text style={s.altToggleText}>
          Bet Alt Lines ({(pitcher.alt_lines ?? []).length}) {expanded ? "▲" : "▼"}
        </Text>
      </Pressable>
      {expanded ? (
        <View style={s.altBody}>
          {(pitcher.alt_lines ?? []).map((alt) => {
            const lineVal = alt.line ?? 0;
            const diff = projKs - lineVal;
            const altKey = `mlb-k-${gamePk}-${pitcher.pitcher_id}-${lineVal}`;
            const isSelected = selectedKeys.has(altKey);
            return (
              <View key={`alt-${lineVal}`} style={s.altRow}>
                <View style={s.altInfo}>
                  <Text style={s.altLine}>Over {fmt(lineVal, 1)}</Text>
                  <Text style={[s.altEdge, { color: diff >= 0.5 ? "#10B981" : diff <= -0.5 ? "#EF4444" : "#94A3B8" }]}>
                    {diff > 0 ? `+${diff.toFixed(1)}` : diff.toFixed(1)} edge
                  </Text>
                  <Text style={s.altPrice}>{fmtOdds(alt.best_price)} {alt.best_book ?? ""}</Text>
                </View>
                <View style={s.altBtns}>
                  <Pressable style={s.altSelectBtn} onPress={() => togglePitcher(pitcher, alt)}>
                    <Text style={s.altSelectText}>{isSelected ? "☑" : "☐"}</Text>
                  </Pressable>
                  <Pressable
                    style={[s.altBookBtn, !getDkUrl(pitcher, alt) ? s.bookBtnDisabled : null]}
                    disabled={!getDkUrl(pitcher, alt)}
                    onPress={() => {
                      addKPropToSlip(pitcher, alt);
                      openBetslip();
                      openUrl(getDkUrl(pitcher, alt));
                    }}
                  >
                    <Text style={s.altBookText}>DK {fmtOdds(alt.dk_price)}</Text>
                  </Pressable>
                  <Pressable
                    style={[s.altBookBtn, !getFdUrl(pitcher, alt) ? s.bookBtnDisabled : null]}
                    disabled={!getFdUrl(pitcher, alt)}
                    onPress={() => {
                      addKPropToSlip(pitcher, alt);
                      openBetslip();
                      openUrl(getFdUrl(pitcher, alt));
                    }}
                  >
                    <Text style={s.altBookText}>FD {fmtOdds(alt.fd_price)}</Text>
                  </Pressable>
                </View>
              </View>
            );
          })}
        </View>
      ) : null}
    </View>
  );
}

// ── Styles ───────────────────────────────────────────────────────────────────

const s = StyleSheet.create({
  screen: { flex: 1, backgroundColor: "#050A18" },
  content: { padding: 16, gap: 10, paddingBottom: 40 },

  // Navigation
  navRow: { flexDirection: "row", gap: 8, marginBottom: 2 },
  navBtn: { borderWidth: StyleSheet.hairlineWidth, borderColor: "#334155", borderRadius: 8, paddingHorizontal: 12, paddingVertical: 6, backgroundColor: "#0F172A" },
  navBtnText: { color: "#93C5FD", fontSize: 12, fontWeight: "700" },

  // Sub-tabs
  tabRow: { flexDirection: "row", gap: 0, marginBottom: 4 },
  tabActive: { flex: 1, borderBottomWidth: 2, borderBottomColor: "#22D3EE", paddingVertical: 10, alignItems: "center" },
  tabInactive: { flex: 1, borderBottomWidth: 2, borderBottomColor: "#1E293B", paddingVertical: 10, alignItems: "center" },
  tabTextActive: { color: "#22D3EE", fontSize: 13, fontWeight: "800" },
  tabTextInactive: { color: "#64748B", fontSize: 13, fontWeight: "700" },

  // Hero
  hero: { borderWidth: StyleSheet.hairlineWidth, borderRadius: 16, backgroundColor: "#071731", padding: 16, gap: 8 },
  eyebrow: { color: "#22D3EE", fontSize: 11, fontWeight: "700" },
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

  // Panel
  panel: { borderWidth: StyleSheet.hairlineWidth, borderRadius: 14, backgroundColor: "#0B1529", padding: 12, gap: 10 },

  // Pitcher header
  pitcherHeader: { flexDirection: "row", alignItems: "center", justifyContent: "space-between", gap: 10 },
  pitcherName: { color: "#E5E7EB", fontSize: 18, fontWeight: "800" },
  pitcherSub: { color: "#94A3B8", fontSize: 12, marginTop: 2 },
  gradePill: { borderWidth: StyleSheet.hairlineWidth, borderRadius: 999, paddingHorizontal: 12, paddingVertical: 5 },
  gradeText: { fontSize: 12, fontWeight: "800" },

  // Line selector
  lineSelectorWrap: { gap: 6 },
  lineSelectorLabel: { color: "#64748B", fontSize: 10, fontWeight: "700" },
  lineSelectorRow: { flexDirection: "row", gap: 6, paddingVertical: 2 },
  lineChip: { borderWidth: 1, borderColor: "#334155", borderRadius: 999, paddingHorizontal: 12, paddingVertical: 6, backgroundColor: "#0F172A" },
  lineChipActive: { borderColor: "#22D3EE", backgroundColor: "rgba(34,211,238,0.15)" },
  lineChipText: { color: "#94A3B8", fontSize: 12, fontWeight: "700" },
  lineChipTextActive: { color: "#22D3EE" },

  // Projection row
  projRow: { flexDirection: "row", justifyContent: "space-between", gap: 4, backgroundColor: "#111827", borderRadius: 10, padding: 10 },
  projBox: { flex: 1, alignItems: "center", gap: 2 },
  projLabel: { color: "#64748B", fontSize: 9, fontWeight: "700" },
  projValue: { color: "#E5E7EB", fontSize: 16, fontWeight: "800" },

  // Active price row
  activePriceRow: { alignItems: "center", paddingVertical: 4 },
  activePriceText: { color: "#BFDBFE", fontSize: 12, fontWeight: "700" },

  // Stats (used inside collapsible cards)
  statsGrid: { flexDirection: "row", flexWrap: "wrap", gap: 0 },
  statItem: { width: "25%", alignItems: "center", paddingVertical: 5 },
  statLabel: { color: "#64748B", fontSize: 9, fontWeight: "700" },
  statVal: { color: "#E5E7EB", fontSize: 13, fontWeight: "700" },

  // Bet section
  betSection: { gap: 6 },
  betLabel: { color: "#BFDBFE", fontSize: 12, fontWeight: "700" },
  betRow: { flexDirection: "row", gap: 8 },
  selectBtn: { flex: 1, borderWidth: 1, borderColor: "#334155", borderRadius: 8, paddingVertical: 8, alignItems: "center", backgroundColor: "#0F172A" },
  selectBtnText: { color: "#93C5FD", fontSize: 12, fontWeight: "700" },
  bookBtn: { flex: 1, borderRadius: 8, paddingVertical: 8, alignItems: "center", backgroundColor: "#1E3A5F" },
  bookBtnDisabled: { opacity: 0.35 },
  bookBtnText: { color: "#E5E7EB", fontSize: 12, fontWeight: "700" },

  // Alt lines
  altSection: { borderWidth: StyleSheet.hairlineWidth, borderColor: "#334155", borderRadius: 10, overflow: "hidden" },
  altToggle: { flexDirection: "row", alignItems: "center", justifyContent: "space-between", paddingHorizontal: 12, paddingVertical: 10, backgroundColor: "#111827" },
  altToggleText: { color: "#93C5FD", fontSize: 12, fontWeight: "700" },
  altBody: { gap: 0 },
  altRow: { flexDirection: "row", alignItems: "center", justifyContent: "space-between", paddingHorizontal: 10, paddingVertical: 8, borderTopWidth: StyleSheet.hairlineWidth, borderTopColor: "#1E293B" },
  altInfo: { flex: 1, gap: 2 },
  altLine: { color: "#E5E7EB", fontSize: 13, fontWeight: "700" },
  altEdge: { fontSize: 11, fontWeight: "700" },
  altPrice: { color: "#94A3B8", fontSize: 11 },
  altBtns: { flexDirection: "row", gap: 6, alignItems: "center" },
  altSelectBtn: { paddingHorizontal: 6, paddingVertical: 4 },
  altSelectText: { color: "#93C5FD", fontSize: 14 },
  altBookBtn: { borderRadius: 6, paddingHorizontal: 8, paddingVertical: 5, backgroundColor: "#1E3A5F" },
  altBookText: { color: "#E5E7EB", fontSize: 10, fontWeight: "700" },

  // Error / empty
  errorBox: { borderWidth: StyleSheet.hairlineWidth, borderRadius: 12, padding: 16, gap: 6, backgroundColor: "#0B1529" },
  errorTitle: { color: "#F87171", fontSize: 14, fontWeight: "800" },
  errorText: { color: "#94A3B8", fontSize: 12 },
  errorRetry: { color: "#93C5FD", fontSize: 12, fontWeight: "700" },
  emptyTitle: { color: "#E5E7EB", fontSize: 15, fontWeight: "700" },
  emptySub: { color: "#94A3B8", fontSize: 12 },
});
