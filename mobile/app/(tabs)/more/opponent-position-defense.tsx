import { useMemo } from "react";
import {
  View,
  Text,
  StyleSheet,
  FlatList,
  ActivityIndicator,
  RefreshControl,
} from "react-native";
import { useTheme } from "@/store/useTheme";
import {
  OpponentPositionDefenseRow,
  useOpponentPositionDefense,
} from "@/hooks/useOpponentPositionDefense";

function formatNumber(value: number | null | undefined, digits = 1) {
  if (value === null || value === undefined || Number.isNaN(value)) {
    return "-";
  }
  return value.toFixed(digits);
}

function formatTimestamp(value?: string) {
  if (!value) return "Unknown";
  const date = new Date(value);
  if (Number.isNaN(date.getTime())) return value;
  return date.toLocaleString();
}

export default function OpponentPositionDefenseScreen() {
  const { colors } = useTheme();
  const { data, loading, error, refetch } =
    useOpponentPositionDefense();

  const sorted = useMemo(() => {
    return [...data].sort((a, b) => {
      const team = a.opponent_team_abbr.localeCompare(
        b.opponent_team_abbr,
      );
      if (team !== 0) return team;
      return a.player_position.localeCompare(b.player_position);
    });
  }, [data]);

  if (loading && data.length === 0) {
    return (
      <View
        style={[
          styles.center,
          { backgroundColor: colors.surface.screen },
        ]}
      >
        <ActivityIndicator color={colors.accent.primary} />
        <Text style={[styles.mutedText, { color: colors.text.muted }]}>
          Loading opponent position defense…
        </Text>
      </View>
    );
  }

  if (error) {
    return (
      <View
        style={[
          styles.center,
          { backgroundColor: colors.surface.screen },
        ]}
      >
        <Text style={[styles.errorText, { color: colors.text.primary }]}>
          Failed to load opponent position defense
        </Text>
        <Text style={[styles.mutedText, { color: colors.text.muted }]}>
          {error}
        </Text>
      </View>
    );
  }

  return (
    <View
      style={[
        styles.container,
        { backgroundColor: colors.surface.screen },
      ]}
    >
      <FlatList
        data={sorted}
        keyExtractor={(item) =>
          `${item.opponent_team_abbr}-${item.player_position}`
        }
        contentContainerStyle={styles.list}
        refreshControl={
          <RefreshControl
            refreshing={loading}
            onRefresh={refetch}
            tintColor={colors.text.secondary}
          />
        }
        renderItem={({ item }) => (
          <DefenseCard item={item} />
        )}
      />
    </View>
  );

  function DefenseCard({ item }: { item: OpponentPositionDefenseRow }) {
    return (
      <View
        style={[
          styles.card,
          {
            backgroundColor: colors.surface.card,
            borderColor: colors.border.subtle,
          },
        ]}
      >
        <View style={styles.headerRow}>
          <Text style={[styles.team, { color: colors.text.primary }]}>
            {item.opponent_team_abbr}
          </Text>
          <Text style={[styles.position, { color: colors.text.secondary }]}>
            {item.player_position}
          </Text>
          <Text style={[styles.games, { color: colors.text.muted }]}>
            GP {item.games_played}
          </Text>
        </View>

        <View style={styles.statRow}>
          <Stat label="PTS" value={formatNumber(item.pts_allowed_avg)} />
          <Stat label="REB" value={formatNumber(item.reb_allowed_avg)} />
          <Stat label="AST" value={formatNumber(item.ast_allowed_avg)} />
          <Stat label="STL" value={formatNumber(item.stl_allowed_avg)} />
          <Stat label="BLK" value={formatNumber(item.blk_allowed_avg)} />
          <Stat label="3PM" value={formatNumber(item.fg3m_allowed_avg)} />
        </View>

        <View style={styles.statRow}>
          <Stat label="PA" value={formatNumber(item.pa_allowed_avg)} />
          <Stat label="PR" value={formatNumber(item.pr_allowed_avg)} />
          <Stat label="PRA" value={formatNumber(item.pra_allowed_avg)} />
          <Stat label="DD%" value={formatNumber(item.dd_rate_allowed, 2)} />
          <Stat label="TD%" value={formatNumber(item.td_rate_allowed, 2)} />
        </View>

        <View style={styles.rankRow}>
          <Rank label="PTS" value={item.pts_allowed_rank} />
          <Rank label="REB" value={item.reb_allowed_rank} />
          <Rank label="AST" value={item.ast_allowed_rank} />
          <Rank label="STL" value={item.stl_allowed_rank} />
          <Rank label="BLK" value={item.blk_allowed_rank} />
          <Rank label="PRA" value={item.pra_allowed_rank} />
        </View>

        <Text style={[styles.updated, { color: colors.text.muted }]}>
          Updated: {formatTimestamp(item.computed_at)}
        </Text>
      </View>
    );
  }

  function Stat({
    label,
    value,
  }: {
    label: string;
    value: string;
  }) {
    return (
      <View style={styles.stat}>
        <Text style={[styles.statLabel, { color: colors.text.muted }]}>
          {label}
        </Text>
        <Text style={[styles.statValue, { color: colors.text.primary }]}>
          {value}
        </Text>
      </View>
    );
  }

  function Rank({
    label,
    value,
  }: {
    label: string;
    value: number;
  }) {
    return (
      <View style={styles.rank}>
        <Text style={[styles.rankLabel, { color: colors.text.muted }]}>
          {label} Rk
        </Text>
        <Text style={[styles.rankValue, { color: colors.text.secondary }]}>
          {value}
        </Text>
      </View>
    );
  }
}

const styles = StyleSheet.create({
  container: {
    flex: 1,
  },
  list: {
    padding: 12,
    gap: 12,
  },
  center: {
    flex: 1,
    alignItems: "center",
    justifyContent: "center",
    gap: 12,
  },
  mutedText: {
    fontSize: 12,
    fontWeight: "600",
  },
  errorText: {
    fontSize: 14,
    fontWeight: "700",
  },
  card: {
    borderRadius: 16,
    borderWidth: 1,
    padding: 14,
    gap: 12,
  },
  headerRow: {
    flexDirection: "row",
    alignItems: "center",
    gap: 8,
  },
  team: {
    fontSize: 18,
    fontWeight: "800",
  },
  position: {
    fontSize: 14,
    fontWeight: "700",
  },
  games: {
    marginLeft: "auto",
    fontSize: 12,
    fontWeight: "600",
  },
  statRow: {
    flexDirection: "row",
    flexWrap: "wrap",
    gap: 12,
  },
  stat: {
    minWidth: 54,
  },
  statLabel: {
    fontSize: 10,
    fontWeight: "700",
    textTransform: "uppercase",
  },
  statValue: {
    fontSize: 14,
    fontWeight: "700",
  },
  rankRow: {
    flexDirection: "row",
    flexWrap: "wrap",
    gap: 10,
  },
  rank: {
    minWidth: 54,
  },
  rankLabel: {
    fontSize: 9,
    fontWeight: "700",
    textTransform: "uppercase",
  },
  rankValue: {
    fontSize: 12,
    fontWeight: "700",
  },
  updated: {
    fontSize: 10,
    fontWeight: "600",
  },
});