import {
  ActivityIndicator,
  Pressable,
  ScrollView,
  StyleSheet,
  Text,
  View,
} from "react-native";
import { Stack } from "expo-router";
import { useCallback, useEffect, useState } from "react";

import { useTheme } from "@/store/useTheme";
import { usePgaBetslip } from "@/store/usePgaBetslip";
import { usePgaBetslipDrawer } from "@/store/usePgaBetslipDrawer";
import { PgaBetslipDrawer } from "@/components/pga/PgaBetslipDrawer";
import { API_BASE } from "@/lib/config";

// ─── Types ───────────────────────────────────────────────────────────────────

type PairingPlayer = {
  player_id: string | null;
  player_id_int: number | null;
  player_display_name: string;
  player_first_name: string;
  player_last_name: string;
  country: string | null;
  world_rank: number | null;
  amateur: boolean;
  player_image_url?: string | null;
};

type AnalyticsPlayer = {
  player_id: number;
  player: { display_name: string };
  rank: number;
  score: number;
  metrics: {
    form_score?: number | null;
    top10_prob?: number | null;
    top20_prob?: number | null;
    head_to_head_win_rate?: number | null;
  };
};

type Recommendation = {
  player_id: number;
  label: string;
  edge: number;
  reasons: string[];
};

type GroupAnalytics = {
  players: AnalyticsPlayer[];
  recommendation?: Recommendation | null;
};

type PairingGroup = {
  group_number: number;
  round_number: number;
  round_status: string | null;
  tee_time: string | null;
  start_hole: number | null;
  back_nine: boolean;
  course_name: string | null;
  players: PairingPlayer[];
  analytics: GroupAnalytics | null;
};

type PairingsResponse = {
  tournament_id: string;
  round_number: number | null;
  snapshot_ts: string | null;
  groups: PairingGroup[];
};

// ─── Helpers ─────────────────────────────────────────────────────────────────

function formatTeeTime(iso: string | null): string {
  if (!iso) return "";
  try {
    const d = new Date(iso);
    return d.toLocaleTimeString(undefined, { hour: "numeric", minute: "2-digit" });
  } catch {
    return iso;
  }
}

function formatPct(v?: number | null): string {
  if (v == null) return "—";
  return `${(v * 100).toFixed(1)}%`;
}

function formatNum(v?: number | null): string {
  if (v == null) return "—";
  return v.toFixed(2);
}

// ─── Sub-components ──────────────────────────────────────────────────────────

function AnalyticsPlayerRow({
  row,
  isRecommended,
  colors,
}: {
  row: AnalyticsPlayer;
  isRecommended: boolean;
  colors: any;
}) {
  return (
    <View
      style={[
        playerRowStyles.wrap,
        {
          borderColor: isRecommended ? "#39E4C9" : colors.border.subtle,
          backgroundColor: isRecommended
            ? "rgba(57,228,201,0.06)"
            : colors.surface.cardSoft,
        },
      ]}
    >
      <View style={playerRowStyles.header}>
        <View style={playerRowStyles.rankBadge}>
          <Text style={[playerRowStyles.rank, { color: isRecommended ? "#39E4C9" : colors.text.muted }]}>
            #{row.rank}
          </Text>
        </View>
        <Text style={[playerRowStyles.name, { color: colors.text.primary }]} numberOfLines={1}>
          {row.player?.display_name ?? `Player ${row.player_id}`}
        </Text>
        {isRecommended && (
          <View style={playerRowStyles.bestBetBadge}>
            <Text style={playerRowStyles.bestBetText}>Best Bet</Text>
          </View>
        )}
      </View>
      <View style={playerRowStyles.metrics}>
        <MetricPill label="Score" value={formatNum(row.score)} colors={colors} />
        <MetricPill label="Form" value={formatNum(row.metrics.form_score)} colors={colors} />
        <MetricPill label="Top10" value={formatPct(row.metrics.top10_prob)} colors={colors} />
        <MetricPill label="H2H%" value={formatPct(row.metrics.head_to_head_win_rate)} colors={colors} />
      </View>
    </View>
  );
}

function MetricPill({
  label,
  value,
  colors,
}: {
  label: string;
  value: string;
  colors: any;
}) {
  return (
    <View style={[metricStyles.pill, { backgroundColor: colors.surface.card, borderColor: colors.border.subtle }]}>
      <Text style={[metricStyles.label, { color: colors.text.muted }]}>{label}</Text>
      <Text style={[metricStyles.value, { color: colors.text.primary }]}>{value}</Text>
    </View>
  );
}

function GroupCard({
  group,
  colors,
  onAddToBetslip,
}: {
  group: PairingGroup;
  colors: any;
  onAddToBetslip: (player: PairingPlayer, group: PairingGroup) => void;
}) {
  const recommendation = group.analytics?.recommendation;
  const analyticsList = group.analytics?.players ?? [];
  const lastNames = group.players.map((p) => p.player_last_name);

  return (
    <View style={[groupStyles.card, { backgroundColor: colors.surface.card, borderColor: colors.border.subtle }]}>
      {/* Header */}
      <View style={groupStyles.header}>
        <View>
          <Text style={[groupStyles.groupLabel, { color: colors.text.muted }]}>
            Group {group.group_number}
          </Text>
          <Text style={[groupStyles.teeTime, { color: colors.text.primary }]}>
            {formatTeeTime(group.tee_time)}
            {group.back_nine ? "  (Back 9)" : ""}
          </Text>
        </View>
        <View style={[groupStyles.roundBadge, { backgroundColor: colors.surface.cardSoft, borderColor: colors.border.subtle }]}>
          <Text style={[groupStyles.roundText, { color: colors.text.muted }]}>
            R{group.round_number}
            {group.round_status ? ` • ${group.round_status}` : ""}
          </Text>
        </View>
      </View>

      {/* Player buttons (last name + save to betslip) */}
      <View style={groupStyles.playerRow}>
        {group.players.map((player) => {
          const isRec =
            recommendation &&
            player.player_id_int === recommendation.player_id;
          return (
            <Pressable
              key={player.player_id ?? player.player_display_name}
              style={[
                groupStyles.playerBtn,
                {
                  backgroundColor: isRec ? "rgba(57,228,201,0.12)" : colors.surface.cardSoft,
                  borderColor: isRec ? "#39E4C9" : colors.border.subtle,
                },
              ]}
              onPress={() => onAddToBetslip(player, group)}
            >
              <Text style={[groupStyles.playerBtnName, { color: isRec ? "#39E4C9" : colors.text.primary }]}>
                {player.player_last_name}
              </Text>
              {isRec && (
                <Text style={groupStyles.recStar}>★</Text>
              )}
              <Text style={[groupStyles.playerBtnSub, { color: colors.text.muted }]}>+ Betslip</Text>
            </Pressable>
          );
        })}
      </View>

      {/* Recommendation summary */}
      {recommendation && (() => {
        const recPlayer = group.players.find(
          (p) => p.player_id_int === recommendation.player_id
        );
        if (!recPlayer) return null;
        return (
          <View style={[groupStyles.recBox, { backgroundColor: "rgba(57,228,201,0.08)", borderColor: "rgba(57,228,201,0.3)" }]}>
            <Text style={groupStyles.recTitle}>
              ★ Best Bet: {recPlayer.player_last_name}
            </Text>
            <Text style={[groupStyles.recReasons, { color: colors.text.muted }]}>
              {recommendation.reasons.slice(0, 3).join("  •  ")}
            </Text>
            <Text style={[groupStyles.recEdge, { color: colors.text.muted }]}>
              Edge: {formatNum(recommendation.edge)}
            </Text>
          </View>
        );
      })()}

      {/* Per-player analytics */}
      {analyticsList.length > 0 && (
        <View style={groupStyles.analyticsSection}>
          {analyticsList.map((row) => (
            <AnalyticsPlayerRow
              key={row.player_id}
              row={row}
              isRecommended={recommendation?.player_id === row.player_id}
              colors={colors}
            />
          ))}
        </View>
      )}
    </View>
  );
}

// ─── Main screen ─────────────────────────────────────────────────────────────

export default function PgaPairingsScreen() {
  const { colors } = useTheme();
  const { add: addToBetslip, items: betslipItems } = usePgaBetslip();
  const { open: openDrawer } = usePgaBetslipDrawer();

  const [data, setData] = useState<PairingsResponse | null>(null);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);

  const fetchPairings = useCallback(async () => {
    setLoading(true);
    setError(null);
    try {
      const url = `${API_BASE}pga/analytics/pairings`;
      const res = await fetch(url, { credentials: "omit" });
      if (!res.ok) {
        const text = await res.text();
        throw new Error(text || `HTTP ${res.status}`);
      }
      const json: PairingsResponse = await res.json();
      setData(json);
    } catch (err: any) {
      setError(err?.message ?? "Failed to load pairings");
    } finally {
      setLoading(false);
    }
  }, []);

  useEffect(() => {
    fetchPairings();
  }, [fetchPairings]);

  const handleAddToBetslip = useCallback(
    (player: PairingPlayer, group: PairingGroup) => {
      const opponents = group.players
        .filter((p) => p.player_id !== player.player_id)
        .map((p) => p.player_last_name);

      addToBetslip({
        id: `pga-${group.group_number}-${group.round_number}-${player.player_id}`,
        playerId: player.player_id,
        playerLastName: player.player_last_name,
        playerDisplayName: player.player_display_name,
        groupPlayers: [player.player_last_name, ...opponents],
        tournamentId: data?.tournament_id,
        teeTime: group.tee_time ? formatTeeTime(group.tee_time) : undefined,
        roundNumber: group.round_number,
        createdAt: new Date().toISOString(),
      });
      openDrawer();
    },
    [addToBetslip, openDrawer, data]
  );

  const groups = data?.groups ?? [];

  return (
    <View style={{ flex: 1, backgroundColor: colors.surface.screen }}>
      <Stack.Screen
        options={{
          title: "PGA Pairings",
          headerStyle: { backgroundColor: colors.surface.screen },
          headerTintColor: colors.text.primary,
        }}
      />

      <ScrollView
        style={{ flex: 1 }}
        contentContainerStyle={styles.content}
        showsVerticalScrollIndicator={false}
      >
        {/* Header */}
        <View style={[styles.heroCard, { borderColor: colors.border.subtle }]}>
          <Text style={styles.heroTitle}>PGA Pairings + Analytics</Text>
          {data?.tournament_id && (
            <Text style={[styles.heroSub, { color: colors.text.muted }]}>
              {data.tournament_id}
              {data.snapshot_ts
                ? `  •  ${new Date(data.snapshot_ts).toLocaleString()}`
                : ""}
            </Text>
          )}
          <Text style={[styles.heroHint, { color: colors.text.muted }]}>
            Tap a player's last name to add them to your betslip.
          </Text>
        </View>

        {loading && (
          <View style={styles.center}>
            <ActivityIndicator color={colors.accent.primary} size="large" />
            <Text style={[styles.loadingText, { color: colors.text.muted }]}>
              Loading pairings + analytics…
            </Text>
          </View>
        )}

        {error && !loading && (
          <View style={styles.center}>
            <Text style={[styles.errorText, { color: colors.text.danger ?? "#ef4444" }]}>
              {error}
            </Text>
            <Pressable
              style={[styles.retryBtn, { backgroundColor: colors.accent.primary }]}
              onPress={fetchPairings}
            >
              <Text style={[styles.retryText, { color: colors.text.inverse }]}>Retry</Text>
            </Pressable>
          </View>
        )}

        {!loading && !error && groups.length === 0 && data && (
          <View style={styles.center}>
            <Text style={[styles.emptyText, { color: colors.text.muted }]}>
              No pairings available.
            </Text>
          </View>
        )}

        {groups.map((group) => (
          <GroupCard
            key={`${group.round_number}-${group.group_number}`}
            group={group}
            colors={colors}
            onAddToBetslip={handleAddToBetslip}
          />
        ))}

        <View style={{ height: 120 }} />
      </ScrollView>

      {/* Betslip floating button */}
      {betslipItems.length > 0 && (
        <Pressable
          style={[styles.fab, { backgroundColor: colors.accent.primary }]}
          onPress={openDrawer}
        >
          <Text style={[styles.fabText, { color: colors.text.inverse }]}>
            Betslip ({betslipItems.length})
          </Text>
        </Pressable>
      )}

      <PgaBetslipDrawer />
    </View>
  );
}

// ─── Styles ──────────────────────────────────────────────────────────────────

const styles = StyleSheet.create({
  content: { padding: 14, paddingBottom: 40 },
  heroCard: {
    borderWidth: StyleSheet.hairlineWidth,
    borderRadius: 16,
    padding: 16,
    backgroundColor: "#071731",
    marginBottom: 14,
  },
  heroTitle: {
    fontSize: 22,
    fontWeight: "800",
    color: "#E9F2FF",
    marginBottom: 4,
  },
  heroSub: { fontSize: 12, marginBottom: 6 },
  heroHint: { fontSize: 12, lineHeight: 17 },
  center: { alignItems: "center", paddingVertical: 40, gap: 12 },
  loadingText: { fontSize: 13, marginTop: 8 },
  errorText: { fontSize: 14, textAlign: "center" },
  retryBtn: {
    paddingHorizontal: 20,
    paddingVertical: 10,
    borderRadius: 10,
    marginTop: 8,
  },
  retryText: { fontSize: 14, fontWeight: "700" },
  emptyText: { fontSize: 14 },
  fab: {
    position: "absolute",
    bottom: 20,
    right: 16,
    paddingHorizontal: 18,
    paddingVertical: 12,
    borderRadius: 24,
    shadowColor: "#000",
    shadowOffset: { width: 0, height: 2 },
    shadowOpacity: 0.25,
    shadowRadius: 6,
    elevation: 5,
  },
  fabText: { fontSize: 14, fontWeight: "800" },
});

const groupStyles = StyleSheet.create({
  card: {
    borderWidth: StyleSheet.hairlineWidth,
    borderRadius: 14,
    padding: 14,
    marginBottom: 14,
    overflow: "hidden",
  },
  header: {
    flexDirection: "row",
    justifyContent: "space-between",
    alignItems: "flex-start",
    marginBottom: 10,
  },
  groupLabel: { fontSize: 11, fontWeight: "700", letterSpacing: 0.5, marginBottom: 2 },
  teeTime: { fontSize: 16, fontWeight: "800" },
  roundBadge: {
    borderWidth: StyleSheet.hairlineWidth,
    borderRadius: 8,
    paddingHorizontal: 8,
    paddingVertical: 4,
  },
  roundText: { fontSize: 11, fontWeight: "600" },
  playerRow: {
    flexDirection: "row",
    gap: 8,
    marginBottom: 10,
    flexWrap: "wrap",
  },
  playerBtn: {
    flex: 1,
    minWidth: 80,
    borderWidth: 1,
    borderRadius: 10,
    paddingVertical: 10,
    paddingHorizontal: 10,
    alignItems: "center",
  },
  playerBtnName: { fontSize: 14, fontWeight: "800" },
  playerBtnSub: { fontSize: 10, marginTop: 3, fontWeight: "600" },
  recStar: { fontSize: 10, color: "#39E4C9", marginTop: 1 },
  recBox: {
    borderWidth: 1,
    borderRadius: 10,
    padding: 10,
    marginBottom: 10,
  },
  recTitle: {
    fontSize: 13,
    fontWeight: "800",
    color: "#39E4C9",
    marginBottom: 3,
  },
  recReasons: { fontSize: 11, lineHeight: 16, marginBottom: 3 },
  recEdge: { fontSize: 11 },
  analyticsSection: { gap: 8 },
});

const playerRowStyles = StyleSheet.create({
  wrap: {
    borderWidth: 1,
    borderRadius: 10,
    padding: 10,
  },
  header: {
    flexDirection: "row",
    alignItems: "center",
    gap: 8,
    marginBottom: 8,
  },
  rankBadge: {
    width: 24,
    alignItems: "center",
  },
  rank: { fontSize: 12, fontWeight: "700" },
  name: { flex: 1, fontSize: 13, fontWeight: "700" },
  bestBetBadge: {
    backgroundColor: "rgba(57,228,201,0.2)",
    borderRadius: 6,
    paddingHorizontal: 6,
    paddingVertical: 2,
  },
  bestBetText: { fontSize: 10, fontWeight: "700", color: "#39E4C9" },
  metrics: { flexDirection: "row", flexWrap: "wrap", gap: 6 },
});

const metricStyles = StyleSheet.create({
  pill: {
    borderWidth: StyleSheet.hairlineWidth,
    borderRadius: 8,
    paddingHorizontal: 8,
    paddingVertical: 5,
    alignItems: "center",
    minWidth: 58,
  },
  label: { fontSize: 9, fontWeight: "600", letterSpacing: 0.3 },
  value: { fontSize: 12, fontWeight: "700", marginTop: 2 },
});
