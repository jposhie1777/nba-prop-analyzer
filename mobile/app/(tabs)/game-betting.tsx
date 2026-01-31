import {
  ScrollView,
  View,
  Text,
  StyleSheet,
  Pressable,
} from "react-native";
import { useTheme } from "@/store/useTheme";
import { useGameBettingAnalytics } from "@/hooks/useGameBettingAnalytics";
import { GameBettingAnalyticsRow } from "@/lib/gameBettingAnalytics";

const formatPercent = (value?: number | null) => {
  if (value === null || value === undefined) return "—";
  return `${Math.round(value * 100)}%`;
};

const formatEdge = (value?: number | null) => {
  if (value === null || value === undefined) return "—";
  const sign = value >= 0 ? "+" : "";
  return `${sign}${(value * 100).toFixed(1)}%`;
};

const formatNumber = (value?: number | null, digits = 1) => {
  if (value === null || value === undefined) return "—";
  return value.toFixed(digits);
};

function GameCard({ game }: { game: GameBettingAnalyticsRow }) {
  const { colors } = useTheme();

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
      <View style={styles.cardHeader}>
        <Text style={[styles.cardTitle, { color: colors.text.primary }]}>
          {`${game.away_team_abbr} @ ${game.home_team_abbr}`}
        </Text>
        <Text style={[styles.cardDate, { color: colors.text.muted }]}>
          {game.start_time_est
            ? new Date(game.start_time_est).toLocaleString()
            : "TBD"}
        </Text>
      </View>

      <View style={styles.cardRow}>
        <View style={styles.statBlock}>
          <Text style={[styles.statLabel, { color: colors.text.muted }]}>
            ML Edge
          </Text>
          <Text style={[styles.statValue, { color: colors.text.primary }]}>
            {game.best_bet_market === "MONEYLINE"
              ? formatEdge(game.best_bet_edge)
              : "—"}
          </Text>
        </View>
        <View style={styles.statBlock}>
          <Text style={[styles.statLabel, { color: colors.text.muted }]}>
            ATS Edge
          </Text>
          <Text style={[styles.statValue, { color: colors.text.primary }]}>
            {game.best_bet_market === "SPREAD"
              ? formatEdge(game.best_bet_edge)
              : "—"}
          </Text>
        </View>
        <View style={styles.statBlock}>
          <Text style={[styles.statLabel, { color: colors.text.muted }]}>
            O/U Edge
          </Text>
          <Text style={[styles.statValue, { color: colors.text.primary }]}>
            {game.best_bet_market === "TOTAL"
              ? formatEdge(game.best_bet_edge)
              : "—"}
          </Text>
        </View>
      </View>

      <View style={styles.cardRow}>
        <View style={styles.statBlock}>
          <Text style={[styles.statLabel, { color: colors.text.muted }]}>
            Home L10 ATS
          </Text>
          <Text style={[styles.statValue, { color: colors.text.primary }]}>
            {formatPercent(game.home_ats_pct_l10)}
          </Text>
        </View>
        <View style={styles.statBlock}>
          <Text style={[styles.statLabel, { color: colors.text.muted }]}>
            Away L10 ATS
          </Text>
          <Text style={[styles.statValue, { color: colors.text.primary }]}>
            {formatPercent(game.away_ats_pct_l10)}
          </Text>
        </View>
        <View style={styles.statBlock}>
          <Text style={[styles.statLabel, { color: colors.text.muted }]}>
            Avg Margin
          </Text>
          <Text style={[styles.statValue, { color: colors.text.primary }]}>
            {formatNumber(game.home_avg_margin_l10)} /{" "}
            {formatNumber(game.away_avg_margin_l10)}
          </Text>
        </View>
      </View>

      <View
        style={[
          styles.bestBetRow,
          { borderTopColor: colors.border.subtle },
        ]}
      >
        <Text style={[styles.bestBetLabel, { color: colors.text.muted }]}>
          Best Bet
        </Text>
        <Text style={[styles.bestBetValue, { color: colors.text.primary }]}>
          {game.best_bet_market
            ? `${game.best_bet_market} · ${game.best_bet_side}`
            : "No clear edge"}
        </Text>
        {game.best_bet_reason ? (
          <Text style={[styles.bestBetReason, { color: colors.text.muted }]}>
            {game.best_bet_reason}
          </Text>
        ) : null}
      </View>
    </View>
  );
}

export default function GameBettingAnalyticsScreen() {
  const { colors } = useTheme();
  const { data, loading, error, refresh } = useGameBettingAnalytics();

  return (
    <ScrollView
      style={{ flex: 1, backgroundColor: colors.surface.screen }}
      contentContainerStyle={{ padding: 16, paddingBottom: 40 }}
    >
      <Text style={[styles.h1, { color: colors.text.primary }]}>
        Game Betting Analytics
      </Text>
      <Text style={[styles.subtitle, { color: colors.text.muted }]}
      >
        Moneyline, spread, and total trends powered by BigQuery game history.
      </Text>

      <View
        style={[
          styles.summaryCard,
          {
            backgroundColor: colors.surface.card,
            borderColor: colors.border.subtle,
          },
        ]}
      >
        <Text style={[styles.summaryTitle, { color: colors.text.primary }]}>
          Model Inputs
        </Text>
        <Text style={[styles.summaryText, { color: colors.text.muted }]}>
          Uses last-10 ATS, O/U, and margin trends per team to highlight
          actionable edges for upcoming games.
        </Text>
        <Pressable
          onPress={refresh}
          style={[
            styles.refreshButton,
            { backgroundColor: colors.accent.primary },
          ]}
        >
          <Text style={[styles.refreshText, { color: colors.text.inverse }]}>
            Refresh
          </Text>
        </Pressable>
      </View>

      {loading ? (
        <Text style={[styles.loadingText, { color: colors.text.muted }]}
        >
          Loading games…
        </Text>
      ) : null}
      {error ? (
        <Text style={[styles.errorText, { color: colors.accent.danger }]}>
          {error}
        </Text>
      ) : null}

      {!loading && data?.games?.length === 0 ? (
        <Text style={[styles.loadingText, { color: colors.text.muted }]}
        >
          No upcoming games found. Try refreshing or adjust filters in the API.
        </Text>
      ) : null}

      {data?.games?.map((game) => (
        <GameCard key={game.game_id} game={game} />
      ))}
    </ScrollView>
  );
}

const styles = StyleSheet.create({
  card: {
    borderWidth: StyleSheet.hairlineWidth,
    borderRadius: 14,
    padding: 14,
    marginBottom: 12,
  },
  cardHeader: {
    marginBottom: 10,
  },
  cardTitle: {
    fontSize: 16,
    fontWeight: "700",
  },
  cardDate: {
    fontSize: 12,
    marginTop: 4,
  },
  cardRow: {
    flexDirection: "row",
    justifyContent: "space-between",
    marginBottom: 10,
  },
  statBlock: {
    flex: 1,
  },
  statLabel: {
    fontSize: 11,
    textTransform: "uppercase",
    marginBottom: 4,
  },
  statValue: {
    fontSize: 14,
    fontWeight: "600",
  },
  bestBetRow: {
    borderTopWidth: StyleSheet.hairlineWidth,
    paddingTop: 10,
  },
  bestBetLabel: {
    fontSize: 11,
    textTransform: "uppercase",
  },
  bestBetValue: {
    fontSize: 14,
    fontWeight: "700",
    marginTop: 4,
  },
  bestBetReason: {
    fontSize: 12,
    marginTop: 2,
  },
  h1: {
    fontSize: 20,
    fontWeight: "800",
    marginBottom: 6,
  },
  subtitle: {
    fontSize: 13,
    marginBottom: 16,
  },
  summaryCard: {
    borderWidth: StyleSheet.hairlineWidth,
    borderRadius: 12,
    padding: 14,
    marginBottom: 16,
  },
  summaryTitle: {
    fontSize: 14,
    fontWeight: "700",
  },
  summaryText: {
    marginTop: 6,
    fontSize: 12,
    lineHeight: 18,
  },
  refreshButton: {
    marginTop: 12,
    borderRadius: 8,
    paddingVertical: 8,
    alignItems: "center",
  },
  refreshText: {
    fontSize: 13,
    fontWeight: "700",
  },
  loadingText: {
    fontSize: 13,
    marginBottom: 12,
  },
  errorText: {
    fontSize: 12,
    marginBottom: 12,
  },
});
