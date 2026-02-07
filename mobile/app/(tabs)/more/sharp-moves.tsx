import {
  ActivityIndicator,
  Pressable,
  RefreshControl,
  ScrollView,
  StyleSheet,
  Text,
  View,
} from "react-native";
import { Stack } from "expo-router";

import { useTheme } from "@/store/useTheme";
import { useSharpMoves } from "@/hooks/useSharpMoves";
import type { SharpMoveGame, BookMovement } from "@/lib/sharpMoves";

// ─── Helpers ─────────────────────────────────────

const alertColors = (level: string, colors: any) => {
  switch (level) {
    case "steam":
      return { bg: colors.accent.danger + "20", border: colors.accent.danger, text: colors.accent.danger };
    case "sharp":
      return { bg: colors.accent.warning + "20", border: colors.accent.warning, text: colors.accent.warning };
    case "notable":
      return { bg: colors.accent.info + "20", border: colors.accent.info, text: colors.accent.info };
    default:
      return { bg: colors.surface.cardSoft, border: colors.border.subtle, text: colors.text.muted };
  }
};

const shiftDisplay = (shift: number | null) => {
  if (shift == null) return "—";
  const sign = shift > 0 ? "+" : "";
  return `${sign}${shift.toFixed(1)}`;
};

const formatSpread = (v: number | null) =>
  v == null ? "—" : v > 0 ? `+${v}` : `${v}`;

const formatTotal = (v: number | null) => (v == null ? "—" : `${v}`);

const formatOdds = (v: number | null) => {
  if (v == null) return "—";
  return v > 0 ? `+${v}` : `${v}`;
};

// ─── Alert Badge ─────────────────────────────────

function AlertBadge({ level }: { level: string }) {
  const { colors } = useTheme();
  const ac = alertColors(level, colors);
  const labels: Record<string, string> = {
    steam: "STEAM MOVE",
    sharp: "SHARP",
    notable: "NOTABLE",
    quiet: "QUIET",
  };

  return (
    <View style={[s.badge, { backgroundColor: ac.bg, borderColor: ac.border }]}>
      <Text style={[s.badgeText, { color: ac.text }]}>
        {labels[level] ?? level.toUpperCase()}
      </Text>
    </View>
  );
}

// ─── Movement Row ────────────────────────────────

function MovementRow({
  label,
  opening,
  current,
  shift,
  movementLabel,
  formatter,
}: {
  label: string;
  opening: number | null;
  current: number | null;
  shift: number | null;
  movementLabel: string;
  formatter: (v: number | null) => string;
}) {
  const { colors } = useTheme();
  const shiftColor =
    movementLabel === "steam" || movementLabel === "sharp"
      ? colors.accent.danger
      : movementLabel === "notable"
      ? colors.accent.warning
      : colors.text.muted;

  return (
    <View style={s.movementRow}>
      <Text style={[s.movementLabel, { color: colors.text.secondary }]}>
        {label}
      </Text>
      <Text style={[s.movementValue, { color: colors.text.muted }]}>
        {formatter(opening)}
      </Text>
      <Text style={[s.arrow, { color: shiftColor }]}>
        {shift != null && shift !== 0 ? (shift > 0 ? "\u2191" : "\u2193") : "\u2192"}
      </Text>
      <Text style={[s.movementValue, { color: colors.text.primary, fontWeight: "800" }]}>
        {formatter(current)}
      </Text>
      <Text style={[s.shiftValue, { color: shiftColor }]}>
        {shiftDisplay(shift)}
      </Text>
    </View>
  );
}

// ─── Book Detail ─────────────────────────────────

function BookDetail({ book }: { book: BookMovement }) {
  const { colors } = useTheme();

  return (
    <View style={[s.bookCard, { backgroundColor: colors.surface.cardSoft }]}>
      <View style={s.bookHeader}>
        <Text style={[s.bookName, { color: colors.text.primary }]}>
          {book.book}
        </Text>
        <Text style={[s.snapCount, { color: colors.text.muted }]}>
          {book.total_snapshots} snapshots
        </Text>
      </View>

      <MovementRow
        label="Spread"
        opening={book.spread.opening}
        current={book.spread.current}
        shift={book.spread.shift}
        movementLabel={book.spread.label}
        formatter={formatSpread}
      />
      <MovementRow
        label="Total"
        opening={book.total.opening}
        current={book.total.current}
        shift={book.total.shift}
        movementLabel={book.total.label}
        formatter={formatTotal}
      />

      {/* Moneyline */}
      <View style={s.mlRow}>
        <Text style={[s.movementLabel, { color: colors.text.secondary }]}>
          ML
        </Text>
        <Text style={[s.mlValue, { color: colors.text.muted }]}>
          H: {formatOdds(book.moneyline.opening_home)}
        </Text>
        <Text style={[s.arrow, { color: colors.text.muted }]}>{"\u2192"}</Text>
        <Text style={[s.mlValue, { color: colors.text.primary }]}>
          {formatOdds(book.moneyline.current_home)}
        </Text>
        <Text style={{ width: 8 }} />
        <Text style={[s.mlValue, { color: colors.text.muted }]}>
          A: {formatOdds(book.moneyline.opening_away)}
        </Text>
        <Text style={[s.arrow, { color: colors.text.muted }]}>{"\u2192"}</Text>
        <Text style={[s.mlValue, { color: colors.text.primary }]}>
          {formatOdds(book.moneyline.current_away)}
        </Text>
      </View>
    </View>
  );
}

// ─── Game Card ───────────────────────────────────

function GameCard({ game }: { game: SharpMoveGame }) {
  const { colors } = useTheme();
  const summary = game.summary;
  const ac = alertColors(summary?.alert_level ?? "quiet", colors);

  return (
    <View
      style={[
        s.gameCard,
        {
          backgroundColor: colors.surface.card,
          borderColor: ac.border,
          borderWidth: summary?.is_sharp || summary?.is_steam ? 2 : 1,
        },
      ]}
    >
      {/* Header */}
      <View style={s.gameHeader}>
        <View style={{ flex: 1 }}>
          <Text style={[s.matchup, { color: colors.text.primary }]}>
            {game.away_team_abbr ?? "?"} @ {game.home_team_abbr ?? "?"}
          </Text>
          {game.game_time_et && (
            <Text style={[s.gameTime, { color: colors.text.muted }]}>
              {game.game_time_et}
            </Text>
          )}
        </View>
        <AlertBadge level={summary?.alert_level ?? "quiet"} />
      </View>

      {/* Summary insights */}
      {summary?.insights && summary.insights.length > 0 && (
        <View style={s.insightsBox}>
          {summary.insights.map((insight, i) => (
            <Text
              key={i}
              style={[s.insightText, { color: ac.text }]}
            >
              {insight}
            </Text>
          ))}
        </View>
      )}

      {/* Summary stats */}
      {summary && (
        <View style={s.statsRow}>
          <View style={s.statItem}>
            <Text style={[s.statLabel, { color: colors.text.muted }]}>
              Spread Move
            </Text>
            <Text
              style={[
                s.statValue,
                {
                  color:
                    summary.max_spread_move >= 1.5
                      ? colors.accent.danger
                      : colors.text.primary,
                },
              ]}
            >
              {summary.avg_spread_shift != null
                ? shiftDisplay(summary.avg_spread_shift)
                : "—"}
            </Text>
          </View>
          <View style={s.statItem}>
            <Text style={[s.statLabel, { color: colors.text.muted }]}>
              Total Move
            </Text>
            <Text
              style={[
                s.statValue,
                {
                  color:
                    summary.max_total_move >= 2.0
                      ? colors.accent.danger
                      : colors.text.primary,
                },
              ]}
            >
              {summary.avg_total_shift != null
                ? shiftDisplay(summary.avg_total_shift)
                : "—"}
            </Text>
          </View>
          <View style={s.statItem}>
            <Text style={[s.statLabel, { color: colors.text.muted }]}>
              Max Spread
            </Text>
            <Text style={[s.statValue, { color: colors.text.primary }]}>
              {summary.max_spread_move.toFixed(1)}
            </Text>
          </View>
          <View style={s.statItem}>
            <Text style={[s.statLabel, { color: colors.text.muted }]}>
              Max Total
            </Text>
            <Text style={[s.statValue, { color: colors.text.primary }]}>
              {summary.max_total_move.toFixed(1)}
            </Text>
          </View>
        </View>
      )}

      {/* Per-book detail */}
      {game.books.map((book, i) => (
        <BookDetail key={`${book.book}-${i}`} book={book} />
      ))}
    </View>
  );
}

// ─── Main Screen ─────────────────────────────────

export default function SharpMovesScreen() {
  const { colors } = useTheme();
  const { data, loading, error, refresh } = useSharpMoves();

  return (
    <>
      <Stack.Screen
        options={{
          title: "Sharp Moves",
          headerStyle: { backgroundColor: colors.surface.screen },
          headerTintColor: colors.text.primary,
        }}
      />
      <ScrollView
        style={{ flex: 1, backgroundColor: colors.surface.screen }}
        contentContainerStyle={s.container}
        refreshControl={
          <RefreshControl refreshing={loading} onRefresh={refresh} />
        }
      >
        <View style={s.headerRow}>
          <View style={{ flex: 1 }}>
            <Text style={[s.pageTitle, { color: colors.text.primary }]}>
              Sharp Moves
            </Text>
            <Text style={[s.pageSubtitle, { color: colors.text.muted }]}>
              Line movement tracker {data?.game_date ? `\u2022 ${data.game_date}` : ""}
            </Text>
          </View>
        </View>

        {/* Summary pills */}
        {data && !loading && (
          <View style={s.pillRow}>
            <View
              style={[
                s.pill,
                { backgroundColor: colors.accent.primary + "15", borderColor: colors.accent.primary + "40" },
              ]}
            >
              <Text style={[s.pillText, { color: colors.accent.primary }]}>
                {data.count} games
              </Text>
            </View>
            {data.sharp_count > 0 && (
              <View
                style={[
                  s.pill,
                  { backgroundColor: colors.accent.danger + "15", borderColor: colors.accent.danger + "40" },
                ]}
              >
                <Text style={[s.pillText, { color: colors.accent.danger }]}>
                  {data.sharp_count} sharp
                </Text>
              </View>
            )}
          </View>
        )}

        {loading && !data && (
          <View style={s.centered}>
            <ActivityIndicator color={colors.accent.primary} />
            <Text style={[s.loadingText, { color: colors.text.muted }]}>
              Loading line movements...
            </Text>
          </View>
        )}

        {error && (
          <View style={s.centered}>
            <Text style={{ color: colors.accent.danger }}>{error}</Text>
            <Pressable
              onPress={refresh}
              style={[s.retryBtn, { borderColor: colors.accent.danger }]}
            >
              <Text style={{ color: colors.accent.danger, fontSize: 12, fontWeight: "700" }}>
                Retry
              </Text>
            </Pressable>
          </View>
        )}

        {!loading && !error && data?.count === 0 && (
          <View style={s.centered}>
            <Text style={{ color: colors.text.muted, fontSize: 14 }}>
              No games scheduled for this date.
            </Text>
          </View>
        )}

        {data?.games.map((game) => (
          <GameCard key={game.game_id} game={game} />
        ))}
      </ScrollView>
    </>
  );
}

// ─── Styles ──────────────────────────────────────

const s = StyleSheet.create({
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
  centered: {
    alignItems: "center",
    paddingVertical: 20,
    gap: 8,
  },
  loadingText: {
    fontSize: 12,
    fontWeight: "600",
  },
  retryBtn: {
    marginTop: 8,
    paddingHorizontal: 16,
    paddingVertical: 8,
    borderWidth: 1,
    borderRadius: 8,
  },

  pillRow: {
    flexDirection: "row",
    gap: 8,
  },
  pill: {
    paddingHorizontal: 14,
    paddingVertical: 6,
    borderRadius: 10,
    borderWidth: 1,
  },
  pillText: {
    fontSize: 12,
    fontWeight: "700",
  },

  // Game card
  gameCard: {
    padding: 12,
    borderRadius: 16,
    gap: 10,
  },
  gameHeader: {
    flexDirection: "row",
    alignItems: "center",
    gap: 8,
  },
  matchup: {
    fontSize: 16,
    fontWeight: "900",
  },
  gameTime: {
    fontSize: 11,
    fontWeight: "600",
    marginTop: 2,
  },

  badge: {
    paddingHorizontal: 10,
    paddingVertical: 4,
    borderRadius: 8,
    borderWidth: 1,
  },
  badgeText: {
    fontSize: 10,
    fontWeight: "800",
    letterSpacing: 0.5,
  },

  insightsBox: {
    gap: 4,
  },
  insightText: {
    fontSize: 12,
    fontWeight: "600",
    paddingLeft: 4,
  },

  statsRow: {
    flexDirection: "row",
    gap: 4,
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
    fontSize: 14,
    fontWeight: "800",
    marginTop: 2,
  },

  // Book detail
  bookCard: {
    padding: 10,
    borderRadius: 10,
    gap: 6,
  },
  bookHeader: {
    flexDirection: "row",
    justifyContent: "space-between",
    alignItems: "center",
  },
  bookName: {
    fontSize: 12,
    fontWeight: "800",
  },
  snapCount: {
    fontSize: 10,
    fontWeight: "600",
  },

  // Movement row
  movementRow: {
    flexDirection: "row",
    alignItems: "center",
    gap: 6,
  },
  movementLabel: {
    fontSize: 11,
    fontWeight: "700",
    width: 50,
  },
  movementValue: {
    fontSize: 12,
    fontWeight: "600",
    minWidth: 36,
    textAlign: "center",
  },
  arrow: {
    fontSize: 14,
    fontWeight: "700",
  },
  shiftValue: {
    fontSize: 11,
    fontWeight: "800",
    minWidth: 32,
    textAlign: "right",
  },

  mlRow: {
    flexDirection: "row",
    alignItems: "center",
    gap: 4,
    flexWrap: "wrap",
  },
  mlValue: {
    fontSize: 11,
    fontWeight: "600",
  },
});
