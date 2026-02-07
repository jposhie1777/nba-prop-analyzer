import {
  ActivityIndicator,
  Pressable,
  ScrollView,
  StyleSheet,
  Text,
  View,
} from "react-native";
import { Stack } from "expo-router";
import { useMemo, useState } from "react";

import { useTheme } from "@/store/useTheme";
import { usePgaQuery } from "@/hooks/pga/usePgaQuery";

// ─── Types ───────────────────────────────────────

type CourseDemand = {
  driving: number;
  approach: number;
  short_game: number;
  putting: number;
  scoring_average: number | null;
  scoring_diff: number | null;
  birdie_rate: number;
  bogey_rate: number;
  description: string;
};

type SGPlayer = {
  player_id: number;
  player: {
    id: number;
    first_name: string;
    last_name: string;
    display_name: string;
  };
  starts: number;
  avg_finish: number;
  avg_par_score: number | null;
  total_sg: number;
  sg_off_the_tee: number;
  sg_approach: number;
  sg_around_green: number;
  sg_putting: number;
  strength_off_the_tee: number;
  strength_approach: number;
  strength_around_green: number;
  strength_putting: number;
  top5_rate: number;
  top10_rate: number;
  top20_rate: number;
  made_cut_rate: number;
  course_fit?: number;
};

type SGResponse = {
  season: number;
  tournament_id: number | null;
  count: number;
  course_demand: CourseDemand | null;
  players: SGPlayer[];
};

// ─── Helpers ─────────────────────────────────────

const formatSG = (v: number) => {
  const sign = v >= 0 ? "+" : "";
  return `${sign}${v.toFixed(2)}`;
};

const formatPct = (v: number) => `${(v * 100).toFixed(0)}%`;

const sgColor = (v: number, colors: any) => {
  if (v >= 0.5) return colors.accent.success;
  if (v > -0.5) return colors.text.primary;
  return colors.accent.danger;
};

type SortKey = "total_sg" | "sg_off_the_tee" | "sg_approach" | "sg_around_green" | "sg_putting" | "course_fit";

// ─── Strength Bar ────────────────────────────────

function StrengthBar({
  label,
  value,
  sg,
}: {
  label: string;
  value: number;
  sg: number;
}) {
  const { colors } = useTheme();
  const barColor =
    value >= 65
      ? colors.accent.success
      : value >= 45
      ? colors.accent.warning
      : colors.accent.danger;

  return (
    <View style={st.strengthRow}>
      <Text style={[st.strengthLabel, { color: colors.text.secondary }]}>
        {label}
      </Text>
      <View style={[st.strengthTrack, { backgroundColor: colors.surface.cardSoft }]}>
        <View
          style={[
            st.strengthFill,
            { width: `${Math.min(value, 100)}%`, backgroundColor: barColor },
          ]}
        />
      </View>
      <Text style={[st.strengthSg, { color: sgColor(sg, colors) }]}>
        {formatSG(sg)}
      </Text>
    </View>
  );
}

// ─── Course Demand Card ──────────────────────────

function CourseDemandCard({ demand }: { demand: CourseDemand }) {
  const { colors } = useTheme();

  const categories = [
    { label: "Driving", value: demand.driving },
    { label: "Approach", value: demand.approach },
    { label: "Short Game", value: demand.short_game },
    { label: "Putting", value: demand.putting },
  ];

  return (
    <View
      style={[
        st.section,
        { backgroundColor: colors.surface.card, borderColor: colors.border.subtle },
      ]}
    >
      <Text style={[st.sectionTitle, { color: colors.text.primary }]}>
        Course Demand Profile
      </Text>
      <Text style={[st.courseDesc, { color: colors.text.secondary }]}>
        {demand.description}
      </Text>

      {categories.map((cat) => (
        <View key={cat.label} style={st.demandRow}>
          <Text style={[st.demandLabel, { color: colors.text.secondary }]}>
            {cat.label}
          </Text>
          <View style={[st.strengthTrack, { backgroundColor: colors.surface.cardSoft }]}>
            <View
              style={[
                st.strengthFill,
                {
                  width: `${cat.value}%`,
                  backgroundColor:
                    cat.value >= 60
                      ? colors.accent.primary
                      : colors.accent.info,
                },
              ]}
            />
          </View>
          <Text style={[st.demandValue, { color: colors.text.muted }]}>
            {cat.value}
          </Text>
        </View>
      ))}

      <View style={st.statsRow}>
        {demand.scoring_average != null && (
          <View style={st.statItem}>
            <Text style={[st.statLabel, { color: colors.text.muted }]}>Scoring Avg</Text>
            <Text style={[st.statValue, { color: colors.text.primary }]}>
              {demand.scoring_average.toFixed(1)}
            </Text>
          </View>
        )}
        <View style={st.statItem}>
          <Text style={[st.statLabel, { color: colors.text.muted }]}>Birdie %</Text>
          <Text style={[st.statValue, { color: colors.accent.success }]}>
            {formatPct(demand.birdie_rate)}
          </Text>
        </View>
        <View style={st.statItem}>
          <Text style={[st.statLabel, { color: colors.text.muted }]}>Bogey %</Text>
          <Text style={[st.statValue, { color: colors.accent.danger }]}>
            {formatPct(demand.bogey_rate)}
          </Text>
        </View>
      </View>
    </View>
  );
}

// ─── Player Card ─────────────────────────────────

function PlayerSGCard({ player, rank }: { player: SGPlayer; rank: number }) {
  const { colors } = useTheme();

  return (
    <View
      style={[
        st.section,
        { backgroundColor: colors.surface.card, borderColor: colors.border.subtle },
      ]}
    >
      <View style={st.playerHeader}>
        <View style={st.rankCircle}>
          <Text style={[st.rankText, { color: colors.accent.primary }]}>
            {rank}
          </Text>
        </View>
        <View style={{ flex: 1 }}>
          <Text style={[st.playerName, { color: colors.text.primary }]}>
            {player.player.display_name ||
              `${player.player.first_name} ${player.player.last_name}`}
          </Text>
          <Text style={[st.playerMeta, { color: colors.text.muted }]}>
            {player.starts} starts | Avg finish: {player.avg_finish}
          </Text>
        </View>
        <View style={st.totalSgBox}>
          <Text style={[st.totalSgLabel, { color: colors.text.muted }]}>Total SG</Text>
          <Text
            style={[
              st.totalSgValue,
              { color: sgColor(player.total_sg, colors) },
            ]}
          >
            {formatSG(player.total_sg)}
          </Text>
        </View>
      </View>

      <StrengthBar
        label="Off the Tee"
        value={player.strength_off_the_tee}
        sg={player.sg_off_the_tee}
      />
      <StrengthBar
        label="Approach"
        value={player.strength_approach}
        sg={player.sg_approach}
      />
      <StrengthBar
        label="Around Green"
        value={player.strength_around_green}
        sg={player.sg_around_green}
      />
      <StrengthBar
        label="Putting"
        value={player.strength_putting}
        sg={player.sg_putting}
      />

      <View style={st.statsRow}>
        <View style={st.statItem}>
          <Text style={[st.statLabel, { color: colors.text.muted }]}>Top 5</Text>
          <Text style={[st.statValue, { color: colors.text.primary }]}>
            {formatPct(player.top5_rate)}
          </Text>
        </View>
        <View style={st.statItem}>
          <Text style={[st.statLabel, { color: colors.text.muted }]}>Top 10</Text>
          <Text style={[st.statValue, { color: colors.text.primary }]}>
            {formatPct(player.top10_rate)}
          </Text>
        </View>
        <View style={st.statItem}>
          <Text style={[st.statLabel, { color: colors.text.muted }]}>Top 20</Text>
          <Text style={[st.statValue, { color: colors.text.primary }]}>
            {formatPct(player.top20_rate)}
          </Text>
        </View>
        <View style={st.statItem}>
          <Text style={[st.statLabel, { color: colors.text.muted }]}>Made Cut</Text>
          <Text style={[st.statValue, { color: colors.text.primary }]}>
            {formatPct(player.made_cut_rate)}
          </Text>
        </View>
      </View>

      {player.course_fit != null && (
        <View style={[st.courseFitBadge, { backgroundColor: colors.accent.primary + "15" }]}>
          <Text style={[st.courseFitText, { color: colors.accent.primary }]}>
            Course Fit: {player.course_fit.toFixed(0)}
          </Text>
        </View>
      )}
    </View>
  );
}

// ─── Sort Chips ──────────────────────────────────

function SortChip({
  label,
  active,
  onPress,
}: {
  label: string;
  active: boolean;
  onPress: () => void;
}) {
  const { colors } = useTheme();
  return (
    <Pressable
      onPress={onPress}
      style={[
        st.chip,
        {
          backgroundColor: active ? colors.accent.primary : colors.surface.cardSoft,
          borderColor: active ? colors.accent.primary : colors.border.subtle,
        },
      ]}
    >
      <Text
        style={[
          st.chipText,
          { color: active ? "#FFFFFF" : colors.text.secondary },
        ]}
      >
        {label}
      </Text>
    </Pressable>
  );
}

// ─── Main Screen ─────────────────────────────────

export default function PgaStrokesGainedScreen() {
  const { colors } = useTheme();
  const [sortBy, setSortBy] = useState<SortKey>("total_sg");

  const { data, loading, error, refetch } = usePgaQuery<SGResponse>(
    "/pga/analytics/strokes-gained",
  );

  const sortedPlayers = useMemo(() => {
    if (!data?.players) return [];
    return [...data.players].sort((a, b) => {
      const aVal = a[sortBy] ?? 0;
      const bVal = b[sortBy] ?? 0;
      return (bVal as number) - (aVal as number);
    });
  }, [data?.players, sortBy]);

  return (
    <>
      <Stack.Screen
        options={{
          title: "Strokes Gained",
          headerStyle: { backgroundColor: colors.surface.screen },
          headerTintColor: colors.text.primary,
        }}
      />
      <ScrollView
        style={{ flex: 1, backgroundColor: colors.surface.screen }}
        contentContainerStyle={st.container}
      >
        <View style={st.headerRow}>
          <View style={{ flex: 1 }}>
            <Text style={[st.pageTitle, { color: colors.text.primary }]}>
              Strokes Gained
            </Text>
            <Text style={[st.pageSubtitle, { color: colors.text.muted }]}>
              SG breakdown by category
              {data?.season ? ` (${data.season})` : ""}
            </Text>
          </View>
          <Pressable
            onPress={refetch}
            style={[
              st.refreshBtn,
              {
                backgroundColor: colors.surface.card,
                borderColor: colors.border.subtle,
              },
            ]}
            disabled={loading}
          >
            <Text style={[st.refreshText, { color: colors.text.primary }]}>
              Refresh
            </Text>
          </Pressable>
        </View>

        {/* Sort chips */}
        <ScrollView
          horizontal
          showsHorizontalScrollIndicator={false}
          contentContainerStyle={st.chipRow}
        >
          <SortChip label="Total SG" active={sortBy === "total_sg"} onPress={() => setSortBy("total_sg")} />
          <SortChip label="Off Tee" active={sortBy === "sg_off_the_tee"} onPress={() => setSortBy("sg_off_the_tee")} />
          <SortChip label="Approach" active={sortBy === "sg_approach"} onPress={() => setSortBy("sg_approach")} />
          <SortChip label="Short Game" active={sortBy === "sg_around_green"} onPress={() => setSortBy("sg_around_green")} />
          <SortChip label="Putting" active={sortBy === "sg_putting"} onPress={() => setSortBy("sg_putting")} />
        </ScrollView>

        {loading && (
          <View style={st.centered}>
            <ActivityIndicator color={colors.accent.primary} />
            <Text style={[st.loadingText, { color: colors.text.muted }]}>
              Loading strokes gained data...
            </Text>
          </View>
        )}

        {error && (
          <View style={st.centered}>
            <Text style={{ color: colors.accent.danger }}>{error}</Text>
          </View>
        )}

        {!loading && !error && data?.course_demand && (
          <CourseDemandCard demand={data.course_demand} />
        )}

        {!loading && !error && sortedPlayers.length === 0 && (
          <View style={st.centered}>
            <Text style={{ color: colors.text.muted }}>
              No strokes gained data available.
            </Text>
          </View>
        )}

        {/* Summary pill */}
        {!loading && !error && sortedPlayers.length > 0 && (
          <View
            style={[
              st.summaryPill,
              {
                backgroundColor: colors.accent.primary + "15",
                borderColor: colors.accent.primary + "40",
              },
            ]}
          >
            <Text style={[st.summaryText, { color: colors.accent.primary }]}>
              {sortedPlayers.length} players analyzed
            </Text>
          </View>
        )}

        {!loading &&
          !error &&
          sortedPlayers.map((player, idx) => (
            <PlayerSGCard
              key={player.player_id}
              player={player}
              rank={idx + 1}
            />
          ))}
      </ScrollView>
    </>
  );
}

// ─── Styles ──────────────────────────────────────

const st = StyleSheet.create({
  container: {
    padding: 12,
    paddingBottom: 24,
    gap: 12,
  },
  headerRow: {
    flexDirection: "row",
    alignItems: "center",
    gap: 12,
  },
  pageTitle: {
    fontSize: 20,
    fontWeight: "900",
  },
  pageSubtitle: {
    fontSize: 12,
    marginTop: 4,
    fontWeight: "600",
  },
  refreshBtn: {
    paddingHorizontal: 12,
    paddingVertical: 8,
    borderRadius: 10,
    borderWidth: 1,
  },
  refreshText: {
    fontSize: 12,
    fontWeight: "700",
  },
  centered: {
    alignItems: "center",
    paddingVertical: 20,
    gap: 8,
  },
  loadingText: {
    fontSize: 12,
    fontWeight: "600",
    marginTop: 4,
  },

  chipRow: {
    flexDirection: "row",
    gap: 8,
    paddingVertical: 4,
  },
  chip: {
    paddingHorizontal: 14,
    paddingVertical: 7,
    borderRadius: 20,
    borderWidth: 1,
  },
  chipText: {
    fontSize: 12,
    fontWeight: "700",
  },

  summaryPill: {
    paddingHorizontal: 14,
    paddingVertical: 8,
    borderRadius: 10,
    borderWidth: 1,
    alignSelf: "flex-start",
  },
  summaryText: {
    fontSize: 12,
    fontWeight: "700",
  },

  section: {
    padding: 12,
    borderRadius: 16,
    borderWidth: 1,
    gap: 8,
  },
  sectionTitle: {
    fontSize: 14,
    fontWeight: "800",
  },
  courseDesc: {
    fontSize: 12,
    fontWeight: "500",
    lineHeight: 17,
  },

  // Demand
  demandRow: {
    flexDirection: "row",
    alignItems: "center",
    gap: 8,
  },
  demandLabel: {
    fontSize: 11,
    fontWeight: "600",
    width: 72,
  },
  demandValue: {
    fontSize: 11,
    fontWeight: "700",
    width: 24,
    textAlign: "right",
  },

  // Player card
  playerHeader: {
    flexDirection: "row",
    alignItems: "center",
    gap: 10,
  },
  rankCircle: {
    width: 28,
    height: 28,
    borderRadius: 14,
    alignItems: "center",
    justifyContent: "center",
  },
  rankText: {
    fontSize: 14,
    fontWeight: "900",
  },
  playerName: {
    fontSize: 14,
    fontWeight: "800",
  },
  playerMeta: {
    fontSize: 11,
    fontWeight: "600",
    marginTop: 2,
  },
  totalSgBox: {
    alignItems: "center",
  },
  totalSgLabel: {
    fontSize: 9,
    fontWeight: "600",
  },
  totalSgValue: {
    fontSize: 16,
    fontWeight: "900",
  },

  // Strength bars
  strengthRow: {
    flexDirection: "row",
    alignItems: "center",
    gap: 8,
  },
  strengthLabel: {
    fontSize: 11,
    fontWeight: "600",
    width: 80,
  },
  strengthTrack: {
    height: 6,
    borderRadius: 3,
    flex: 1,
    overflow: "hidden",
  },
  strengthFill: {
    height: "100%",
    borderRadius: 3,
  },
  strengthSg: {
    fontSize: 11,
    fontWeight: "700",
    width: 40,
    textAlign: "right",
  },

  // Stats row
  statsRow: {
    flexDirection: "row",
    gap: 8,
  },
  statItem: {
    flex: 1,
    alignItems: "center",
  },
  statLabel: {
    fontSize: 9,
    fontWeight: "600",
  },
  statValue: {
    fontSize: 13,
    fontWeight: "700",
    marginTop: 2,
  },

  courseFitBadge: {
    paddingHorizontal: 10,
    paddingVertical: 4,
    borderRadius: 8,
    alignSelf: "flex-start",
  },
  courseFitText: {
    fontSize: 11,
    fontWeight: "700",
  },
});
