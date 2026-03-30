import { useCallback, useMemo, useState } from "react";
import {
  ActivityIndicator,
  Linking,
  Pressable,
  ScrollView,
  StyleSheet,
  Text,
  View,
} from "react-native";
import { useLocalSearchParams, useRouter } from "expo-router";

import {
  ParlayLeg,
  SoccerMarket,
  SoccerSelection,
  buildParlayLink,
  useSoccerAnalytics,
} from "@/hooks/soccer/useSoccerFdAnalytics";
import { useTheme } from "@/store/useTheme";

// ─── Helpers ──────────────────────────────────────────────────────────────────

function fmtPct(v?: number | null): string {
  if (v == null) return "–";
  return `${(v * 100).toFixed(1)}%`;
}

function fmtNum(v?: number | null, digits = 2): string {
  if (v == null) return "–";
  return v.toFixed(digits);
}

function fmtEdge(v?: number | null): string {
  if (v == null) return "–";
  const sign = v >= 0 ? "+" : "";
  return `${sign}${v.toFixed(3)}`;
}

function fmtOdds(v?: number | null): string {
  if (v == null) return "–";
  return v > 0 ? `+${v}` : `${v}`;
}

function fmtKickoff(ts?: string | null): string {
  if (!ts) return "–";
  try {
    return new Date(ts).toLocaleString(undefined, {
      weekday: "short",
      month: "short",
      day: "numeric",
      hour: "numeric",
      minute: "2-digit",
    });
  } catch {
    return ts;
  }
}

const MAX_PARLAY_LEGS = 12;

// ─── Edge badge ───────────────────────────────────────────────────────────────

function EdgeBadge({ tier }: { tier?: string | null }) {
  if (!tier) return null;
  const bg =
    tier === "Strong"
      ? "rgba(74,222,128,0.18)"
      : tier === "Medium"
        ? "rgba(251,191,36,0.18)"
        : "rgba(148,163,184,0.12)";
  const fg =
    tier === "Strong" ? "#86EFAC" : tier === "Medium" ? "#FDE68A" : "#94A3B8";
  return (
    <View style={[styles.edgeBadge, { backgroundColor: bg }]}>
      <Text style={[styles.edgeBadgeText, { color: fg }]}>{tier}</Text>
    </View>
  );
}

// ─── Model signals card ───────────────────────────────────────────────────────

function ModelCard({ model, colors }: { model: any; colors: any }) {
  const edge = model.home_win_form_edge;
  const edgeColor =
    edge == null ? colors.text.primary : edge >= 0 ? "#4ADE80" : "#F87171";

  return (
    <View style={[styles.section, { borderColor: colors.border.subtle }]}>
      <Text style={styles.sectionTitle}>MODEL SIGNALS</Text>
      <View style={styles.modelGrid}>
        <ModelStat label="Expected Goals" value={fmtNum(model.expected_total_goals)} colors={colors} />
        <ModelStat label="xG Total" value={fmtNum(model.xg_total)} colors={colors} />
        <ModelStat label="BTTS Prob" value={fmtPct(model.btts_probability)} colors={colors} />
        <ModelStat label="Exp Corners" value={fmtNum(model.expected_corners)} colors={colors} />
        <ModelStat label="Exp Cards" value={fmtNum(model.expected_cards)} colors={colors} />
        <View style={styles.modelStatItem}>
          <Text style={[styles.modelStatVal, { color: edgeColor }]}>
            {fmtEdge(edge)}
          </Text>
          <Text style={[styles.modelStatLabel, { color: colors.text.muted }]}>
            Home Form Edge
          </Text>
        </View>
      </View>
    </View>
  );
}

function ModelStat({ label, value, colors }: { label: string; value: string; colors: any }) {
  return (
    <View style={styles.modelStatItem}>
      <Text style={[styles.modelStatVal, { color: colors.text.primary }]}>{value}</Text>
      <Text style={[styles.modelStatLabel, { color: colors.text.muted }]}>{label}</Text>
    </View>
  );
}

// ─── Form stats card ──────────────────────────────────────────────────────────

function FormCard({
  form,
  homeTeam,
  awayTeam,
  colors,
}: {
  form: any;
  homeTeam: string;
  awayTeam: string;
  colors: any;
}) {
  const rows: { label: string; hKey: keyof typeof form.home; better: "higher" | "lower" | null }[] = [
    { label: "L5 Goals/Game", hKey: "l5_goals_pg", better: "higher" },
    { label: "L5 Goals Allowed", hKey: "l5_goals_allowed_pg", better: "lower" },
    { label: "L5 Win Rate", hKey: "l5_win_rate", better: "higher" },
    { label: "L5 Draw Rate", hKey: "l5_draw_rate", better: null },
    { label: "L5 BTTS Rate", hKey: "l5_btts_rate", better: null },
    { label: "L5 Corners/Game", hKey: "l5_corners_pg", better: "higher" },
    { label: "L5 Cards/Game", hKey: "l5_cards_pg", better: null },
  ];

  return (
    <View style={[styles.section, { borderColor: colors.border.subtle }]}>
      <Text style={styles.sectionTitle}>FORM STATS (L5)</Text>
      {/* Column headers */}
      <View style={styles.formRow}>
        <Text style={[styles.formTeamHeader, { color: "#22D3EE" }]} numberOfLines={1}>
          {homeTeam}
        </Text>
        <Text style={[styles.formStatLabel, { color: colors.text.muted }]} />
        <Text style={[styles.formTeamHeader, { color: "#A78BFA", textAlign: "right" }]} numberOfLines={1}>
          {awayTeam}
        </Text>
      </View>
      {rows.map(({ label, hKey, better }) => {
        const hv = form.home[hKey] as number | null | undefined;
        const av = form.away[hKey] as number | null | undefined;
        const isRate = hKey.includes("rate") || hKey.includes("btts");
        const fmt = (v: number | null | undefined) =>
          isRate ? fmtPct(v) : fmtNum(v);
        const homeWins =
          better === "higher"
            ? (hv ?? -Infinity) > (av ?? -Infinity)
            : better === "lower"
              ? (hv ?? Infinity) < (av ?? Infinity)
              : false;
        const awayWins =
          better === "higher"
            ? (av ?? -Infinity) > (hv ?? -Infinity)
            : better === "lower"
              ? (av ?? Infinity) < (hv ?? Infinity)
              : false;
        return (
          <View key={label} style={styles.formRow}>
            <Text
              style={[
                styles.formVal,
                homeWins ? styles.formHighlight : { color: colors.text.primary },
              ]}
            >
              {fmt(hv)}
            </Text>
            <Text style={[styles.formStatLabel, { color: colors.text.muted }]}>{label}</Text>
            <Text
              style={[
                styles.formVal,
                { textAlign: "right" },
                awayWins ? styles.formHighlight : { color: colors.text.primary },
              ]}
            >
              {fmt(av)}
            </Text>
          </View>
        );
      })}
    </View>
  );
}

// ─── Selection row ────────────────────────────────────────────────────────────

function SelectionRow({
  sel,
  marketId,
  inParlay,
  parlayFull,
  onBet,
  onToggleParlay,
  colors,
}: {
  sel: SoccerSelection;
  marketId: string;
  inParlay: boolean;
  parlayFull: boolean;
  onBet: () => void;
  onToggleParlay: () => void;
  colors: any;
}) {
  return (
    <View style={[styles.selRow, { borderColor: colors.border.subtle }]}>
      <View style={{ flex: 1 }}>
        <View style={styles.selNameRow}>
          <Text style={[styles.selName, { color: colors.text.primary }]}>
            {sel.selection_name}
            {sel.handicap != null ? ` (${sel.handicap > 0 ? "+" : ""}${sel.handicap})` : ""}
          </Text>
          <EdgeBadge tier={sel.model_edge_tier} />
        </View>
        <View style={styles.selMetaRow}>
          <Text style={[styles.selOdds, { color: "#86EFAC" }]}>
            {fmtOdds(sel.odds_american)}
          </Text>
          <Text style={[styles.selProb, { color: colors.text.muted }]}>
            {fmtPct(sel.implied_probability)} implied
          </Text>
          {sel.no_vig_probability != null && (
            <Text style={[styles.selProb, { color: colors.text.muted }]}>
              {fmtPct(sel.no_vig_probability)} no-vig
            </Text>
          )}
          {sel.model_total_line_edge != null && (
            <Text style={[styles.selEdge, { color: sel.model_total_line_edge >= 0 ? "#4ADE80" : "#F87171" }]}>
              edge {fmtEdge(sel.model_total_line_edge)}
            </Text>
          )}
        </View>
      </View>
      <View style={styles.selActions}>
        {/* Parlay toggle */}
        <Pressable
          style={[
            styles.parlayToggleBtn,
            {
              borderColor: inParlay ? "#A78BFA" : colors.border.subtle,
              backgroundColor: inParlay ? "rgba(167,139,250,0.18)" : "transparent",
            },
          ]}
          onPress={onToggleParlay}
          disabled={!inParlay && parlayFull}
        >
          <Text style={{ color: inParlay ? "#A78BFA" : colors.text.muted, fontSize: 16 }}>
            {inParlay ? "✓" : "+"}
          </Text>
        </Pressable>
        {/* Bet single */}
        {sel.fd_deep_link ? (
          <Pressable
            style={[styles.betBtn, { borderColor: colors.border.subtle }]}
            onPress={onBet}
          >
            <Text style={[styles.betBtnText, { color: colors.text.primary }]}>Bet</Text>
          </Pressable>
        ) : null}
      </View>
    </View>
  );
}

// ─── Market card ──────────────────────────────────────────────────────────────

function MarketCard({
  market,
  parlayLegs,
  onToggleLeg,
  colors,
}: {
  market: SoccerMarket;
  parlayLegs: ParlayLeg[];
  onToggleLeg: (leg: ParlayLeg) => void;
  colors: any;
}) {
  const parlayFull = parlayLegs.length >= MAX_PARLAY_LEGS;

  return (
    <View style={[styles.marketCard, { borderColor: colors.border.subtle }]}>
      <Text style={[styles.marketName, { color: colors.text.primary }]}>{market.market_name}</Text>
      {market.selections.map((sel) => {
        const legId = `${market.fd_market_id}-${sel.fd_selection_id}`;
        const inParlay = parlayLegs.some(
          (l) => l.fd_market_id === market.fd_market_id && l.fd_selection_id === sel.fd_selection_id
        );
        return (
          <SelectionRow
            key={sel.fd_selection_id ?? legId}
            sel={sel}
            marketId={market.fd_market_id}
            inParlay={inParlay}
            parlayFull={parlayFull}
            colors={colors}
            onBet={() => {
              if (sel.fd_deep_link) Linking.openURL(sel.fd_deep_link);
            }}
            onToggleParlay={() =>
              onToggleLeg({
                fd_market_id: market.fd_market_id,
                fd_selection_id: sel.fd_selection_id,
                selection_name: sel.selection_name ?? "",
                market_name: market.market_name,
                odds_american: sel.odds_american,
              })
            }
          />
        );
      })}
    </View>
  );
}

// ─── Parlay bottom bar ────────────────────────────────────────────────────────

function ParlayBar({
  legs,
  onBuildParlay,
  onClear,
}: {
  legs: ParlayLeg[];
  onBuildParlay: () => void;
  onClear: () => void;
}) {
  if (legs.length === 0) return null;
  return (
    <View style={styles.parlayBar}>
      <View style={styles.parlayBarLeft}>
        <Text style={styles.parlayCount}>{legs.length} Leg{legs.length !== 1 ? "s" : ""} Selected</Text>
        <Text style={styles.parlayLegsPreview} numberOfLines={1}>
          {legs.map((l) => l.selection_name).join(" · ")}
        </Text>
      </View>
      <View style={styles.parlayBarButtons}>
        <Pressable style={styles.parlayBuildBtn} onPress={onBuildParlay}>
          <Text style={styles.parlayBuildBtnText}>Build Parlay</Text>
        </Pressable>
        <Pressable style={styles.parlayClearBtn} onPress={onClear}>
          <Text style={styles.parlayClearBtnText}>Clear</Text>
        </Pressable>
      </View>
    </View>
  );
}

// ─── Main screen ───────────────────────────────────────────────────────────────

export default function SoccerGameDetailScreen() {
  const { colors } = useTheme();
  const router = useRouter();
  const params = useLocalSearchParams<{
    eventId: string;
    league: string;
    game: string;
    homeTeam: string;
    awayTeam: string;
    eventStartTs: string;
  }>();

  const { eventId, league, homeTeam, awayTeam, eventStartTs } = params;

  const { data, loading, error, refetch } = useSoccerAnalytics(league, eventId);

  const [parlayLegs, setParlayLegs] = useState<ParlayLeg[]>([]);

  const toggleLeg = useCallback((leg: ParlayLeg) => {
    setParlayLegs((prev) => {
      const existing = prev.findIndex(
        (l) => l.fd_market_id === leg.fd_market_id && l.fd_selection_id === leg.fd_selection_id
      );
      if (existing >= 0) {
        return prev.filter((_, i) => i !== existing);
      }
      if (prev.length >= MAX_PARLAY_LEGS) return prev;
      return [...prev, leg];
    });
  }, []);

  const handleBuildParlay = useCallback(() => {
    if (parlayLegs.length === 0) return;
    const url = buildParlayLink(parlayLegs);
    Linking.openURL(url);
  }, [parlayLegs]);

  const clearParlay = useCallback(() => setParlayLegs([]), []);

  // Group markets by type for display order
  const markets = useMemo(() => data?.markets ?? [], [data]);

  const resolvedHome = data?.home_team ?? homeTeam ?? "";
  const resolvedAway = data?.away_team ?? awayTeam ?? "";
  const resolvedTs = data?.event_start_ts ?? eventStartTs ?? "";

  return (
    <View style={{ flex: 1, backgroundColor: "#050A18" }}>
      <ScrollView contentContainerStyle={styles.content}>
        {/* Header */}
        <View style={[styles.header, { borderColor: colors.border.subtle }]}>
          <View style={styles.headerTeams}>
            <Text style={[styles.headerTeam, { color: colors.text.primary }]}>{resolvedHome}</Text>
            <Text style={[styles.headerVs, { color: colors.text.muted }]}>vs</Text>
            <Text style={[styles.headerTeam, { color: colors.text.primary }]}>{resolvedAway}</Text>
          </View>
          <Text style={[styles.headerKickoff, { color: colors.text.muted }]}>
            {fmtKickoff(resolvedTs)}
          </Text>
          {(data?.league ?? league) ? (
            <View style={styles.headerLeagueBadge}>
              <Text style={styles.headerLeagueBadgeText}>{data?.league ?? league}</Text>
            </View>
          ) : null}
        </View>

        {loading && <ActivityIndicator color="#A78BFA" style={{ marginTop: 24 }} />}

        {error && (
          <Pressable
            onPress={refetch}
            style={[styles.errorBox, { borderColor: colors.border.subtle }]}
          >
            <Text style={styles.errorTitle}>Failed to load analytics</Text>
            <Text style={[styles.sub, { color: colors.text.muted }]}>{error}</Text>
            <Text style={styles.errorRetry}>Tap to retry</Text>
          </Pressable>
        )}

        {data && (
          <>
            {/* Model Signals */}
            <ModelCard model={data.model} colors={colors} />

            {/* Form Stats */}
            <FormCard
              form={data.form}
              homeTeam={resolvedHome}
              awayTeam={resolvedAway}
              colors={colors}
            />

            {/* Markets */}
            <View style={[styles.section, { borderColor: colors.border.subtle }]}>
              <Text style={styles.sectionTitle}>MARKETS</Text>
              <Text style={[styles.parlayHint, { color: colors.text.muted }]}>
                Tap + to add legs to parlay builder (max {MAX_PARLAY_LEGS})
              </Text>
              {markets.map((market) => (
                <MarketCard
                  key={market.fd_market_id || market.market_name}
                  market={market}
                  parlayLegs={parlayLegs}
                  onToggleLeg={toggleLeg}
                  colors={colors}
                />
              ))}
              {markets.length === 0 && !loading && (
                <Text style={[styles.sub, { color: colors.text.muted }]}>
                  No markets available for this game.
                </Text>
              )}
            </View>
          </>
        )}

        {/* Bottom padding so parlay bar doesn't cover last market */}
        {parlayLegs.length > 0 && <View style={{ height: 90 }} />}
      </ScrollView>

      {/* Sticky parlay bar */}
      <ParlayBar
        legs={parlayLegs}
        onBuildParlay={handleBuildParlay}
        onClear={clearParlay}
      />
    </View>
  );
}

// ─── Styles ────────────────────────────────────────────────────────────────────

const styles = StyleSheet.create({
  content: { padding: 14, gap: 12, paddingBottom: 40 },

  // Header
  header: {
    borderWidth: StyleSheet.hairlineWidth,
    borderRadius: 16,
    backgroundColor: "#071731",
    padding: 16,
    gap: 6,
  },
  headerTeams: { flexDirection: "row", alignItems: "center", flexWrap: "wrap", gap: 8 },
  headerTeam: { fontSize: 18, fontWeight: "800" },
  headerVs: { fontSize: 13, fontWeight: "600" },
  headerKickoff: { fontSize: 12 },
  headerLeagueBadge: {
    alignSelf: "flex-start",
    borderRadius: 999,
    backgroundColor: "rgba(167,139,250,0.18)",
    paddingHorizontal: 10,
    paddingVertical: 3,
  },
  headerLeagueBadgeText: { color: "#A78BFA", fontSize: 10, fontWeight: "800" },

  // Sections
  section: {
    borderWidth: StyleSheet.hairlineWidth,
    borderRadius: 14,
    backgroundColor: "#0B1529",
    padding: 14,
    gap: 10,
  },
  sectionTitle: {
    color: "#90B3E9",
    fontSize: 11,
    fontWeight: "700",
    letterSpacing: 1,
  },

  // Model grid
  modelGrid: { flexDirection: "row", flexWrap: "wrap", gap: 14 },
  modelStatItem: { alignItems: "center", minWidth: 80 },
  modelStatVal: { fontSize: 18, fontWeight: "800" },
  modelStatLabel: { fontSize: 10, fontWeight: "600", marginTop: 2, textAlign: "center" },

  // Form
  formRow: {
    flexDirection: "row",
    alignItems: "center",
    justifyContent: "space-between",
  },
  formTeamHeader: { fontSize: 11, fontWeight: "800", flex: 1 },
  formStatLabel: { fontSize: 11, flex: 1.6, textAlign: "center" },
  formVal: { fontSize: 12, fontWeight: "700", flex: 1 },
  formHighlight: { color: "#4ADE80" },

  // Market cards
  marketCard: {
    borderWidth: StyleSheet.hairlineWidth,
    borderRadius: 12,
    padding: 10,
    gap: 6,
    marginTop: 4,
  },
  marketName: { fontSize: 13, fontWeight: "700", marginBottom: 2 },

  // Selection rows
  selRow: {
    flexDirection: "row",
    alignItems: "center",
    borderWidth: StyleSheet.hairlineWidth,
    borderRadius: 10,
    padding: 10,
    backgroundColor: "rgba(148,163,184,0.05)",
    gap: 8,
  },
  selNameRow: { flexDirection: "row", alignItems: "center", gap: 6, flexWrap: "wrap" },
  selName: { fontSize: 13, fontWeight: "700" },
  selMetaRow: { flexDirection: "row", flexWrap: "wrap", gap: 8, marginTop: 3 },
  selOdds: { fontSize: 14, fontWeight: "800" },
  selProb: { fontSize: 11 },
  selEdge: { fontSize: 11, fontWeight: "700" },
  selActions: { flexDirection: "row", alignItems: "center", gap: 6 },
  parlayToggleBtn: {
    width: 30,
    height: 30,
    borderRadius: 999,
    borderWidth: StyleSheet.hairlineWidth,
    alignItems: "center",
    justifyContent: "center",
  },
  betBtn: {
    borderWidth: StyleSheet.hairlineWidth,
    borderRadius: 8,
    paddingHorizontal: 10,
    paddingVertical: 5,
  },
  betBtnText: { fontSize: 12, fontWeight: "700" },

  // Edge badge
  edgeBadge: { borderRadius: 999, paddingHorizontal: 8, paddingVertical: 2 },
  edgeBadgeText: { fontSize: 10, fontWeight: "800" },

  // Parlay hint
  parlayHint: { fontSize: 11 },

  // Parlay bar (sticky bottom)
  parlayBar: {
    position: "absolute",
    bottom: 0,
    left: 0,
    right: 0,
    backgroundColor: "#0F1B35",
    borderTopWidth: 1,
    borderTopColor: "rgba(167,139,250,0.35)",
    padding: 14,
    flexDirection: "row",
    alignItems: "center",
    justifyContent: "space-between",
    gap: 10,
  },
  parlayBarLeft: { flex: 1, gap: 2 },
  parlayCount: { color: "#A78BFA", fontSize: 13, fontWeight: "800" },
  parlayLegsPreview: { color: "#94A3B8", fontSize: 10 },
  parlayBarButtons: { flexDirection: "row", gap: 8 },
  parlayBuildBtn: {
    backgroundColor: "#7C3AED",
    borderRadius: 10,
    paddingHorizontal: 14,
    paddingVertical: 8,
  },
  parlayBuildBtnText: { color: "#fff", fontWeight: "800", fontSize: 13 },
  parlayClearBtn: {
    borderWidth: 1,
    borderColor: "rgba(148,163,184,0.3)",
    borderRadius: 10,
    paddingHorizontal: 12,
    paddingVertical: 8,
  },
  parlayClearBtnText: { color: "#94A3B8", fontWeight: "700", fontSize: 12 },

  // Error / sub
  errorBox: {
    borderWidth: StyleSheet.hairlineWidth,
    borderRadius: 12,
    backgroundColor: "#1F2937",
    padding: 14,
  },
  errorTitle: { color: "#FCA5A5", fontWeight: "700" },
  errorRetry: { color: "#E5E7EB", marginTop: 8, fontSize: 12 },
  sub: { fontSize: 12, lineHeight: 18 },
});
