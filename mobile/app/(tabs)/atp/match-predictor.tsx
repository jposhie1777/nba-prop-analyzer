import {
  ActivityIndicator,
  ScrollView,
  StyleSheet,
  Text,
  TextInput,
  View,
} from "react-native";
import { Stack } from "expo-router";
import { useEffect, useMemo, useState } from "react";

import { useTheme } from "@/store/useTheme";
import { useAtpPlayers } from "@/hooks/atp/useAtpPlayers";
import { useAtpQuery } from "@/hooks/atp/useAtpQuery";
import { SearchPicker, SearchItem } from "@/components/pga/SearchPicker";

// ─── Types ───────────────────────────────────────

type SetScores = Record<string, number>;

type Prediction = {
  win_probability: number;
  loss_probability: number;
  confidence: string;
  predicted_winner_id: number;
  predicted_winner_name: string | null;
  set_scores: SetScores;
  tiebreak_probability: number;
};

type PlayerFactor = {
  player_id: number;
  name: string | null;
  composite_score: number;
  ranking: number | null;
  form_score: number | null;
  win_rate: number | null;
  straight_sets_rate: number | null;
  tiebreak_rate: number | null;
  recent_results: string[];
  surface_record: { matches: number; win_rate: number } | null;
};

type H2HFactor = {
  starts: number;
  wins: number;
  losses: number;
  win_rate: number;
  by_surface: Array<{
    surface: string;
    matches: number;
    wins: number;
    losses: number;
    win_rate: number;
  }>;
};

type MatchPredictorResponse = {
  player_id: number;
  opponent_id: number;
  player_name: string | null;
  opponent_name: string | null;
  surface: string | null;
  best_of_5: boolean;
  prediction: Prediction;
  factors: {
    player: PlayerFactor;
    opponent: PlayerFactor;
    head_to_head: H2HFactor;
    weights: Record<string, number>;
  };
  insights: string[];
};

// ─── Helpers ─────────────────────────────────────

const formatPct = (v?: number | null) =>
  v == null ? "—" : `${(v * 100).toFixed(0)}%`;

const confidenceColor = (conf: string, colors: any): string => {
  switch (conf) {
    case "High":
      return colors.accent.success;
    case "Moderate":
      return colors.accent.warning;
    default:
      return colors.accent.info;
  }
};

// ─── Probability Bar ─────────────────────────────

function ProbBar({
  prob,
  label,
  color,
}: {
  prob: number;
  label: string;
  color: string;
}) {
  const { colors } = useTheme();
  return (
    <View style={s.probRow}>
      <Text style={[s.probLabel, { color: colors.text.secondary }]}>
        {label}
      </Text>
      <View style={[s.barTrack, { backgroundColor: colors.surface.cardSoft }]}>
        <View
          style={[s.barFill, { width: `${Math.round(prob * 100)}%`, backgroundColor: color }]}
        />
      </View>
      <Text style={[s.probValue, { color }]}>{formatPct(prob)}</Text>
    </View>
  );
}

// ─── Set Score Distribution ──────────────────────

function SetScoreCard({ scores }: { scores: SetScores }) {
  const { colors } = useTheme();
  const sorted = Object.entries(scores).sort(([, a], [, b]) => b - a);

  return (
    <View style={[s.section, { backgroundColor: colors.surface.card, borderColor: colors.border.subtle }]}>
      <Text style={[s.sectionTitle, { color: colors.text.primary }]}>
        Predicted Set Score
      </Text>
      {sorted.map(([score, prob]) => (
        <View key={score} style={s.setRow}>
          <Text style={[s.setScore, { color: colors.text.primary }]}>
            {score}
          </Text>
          <View style={[s.barTrack, { backgroundColor: colors.surface.cardSoft, flex: 1 }]}>
            <View
              style={[
                s.barFill,
                {
                  width: `${Math.round(prob * 100)}%`,
                  backgroundColor: colors.accent.primary,
                },
              ]}
            />
          </View>
          <Text style={[s.setPct, { color: colors.text.muted }]}>
            {formatPct(prob)}
          </Text>
        </View>
      ))}
    </View>
  );
}

// ─── Player Factor Card ──────────────────────────

function FactorCard({
  factor,
  isWinner,
}: {
  factor: PlayerFactor;
  isWinner: boolean;
}) {
  const { colors } = useTheme();

  return (
    <View
      style={[
        s.section,
        {
          backgroundColor: colors.surface.card,
          borderColor: isWinner ? colors.accent.success : colors.border.subtle,
          borderWidth: isWinner ? 2 : 1,
        },
      ]}
    >
      <View style={s.factorHeader}>
        <Text style={[s.factorName, { color: colors.text.primary }]}>
          {factor.name ?? `Player ${factor.player_id}`}
        </Text>
        {factor.ranking && (
          <Text style={[s.rankBadge, { color: colors.accent.primary }]}>
            #{factor.ranking}
          </Text>
        )}
      </View>

      <View style={s.metricsGrid}>
        <View style={s.metricItem}>
          <Text style={[s.metricLabel, { color: colors.text.muted }]}>Form</Text>
          <Text style={[s.metricValue, { color: colors.text.primary }]}>
            {factor.form_score?.toFixed(2) ?? "—"}
          </Text>
        </View>
        <View style={s.metricItem}>
          <Text style={[s.metricLabel, { color: colors.text.muted }]}>Win %</Text>
          <Text style={[s.metricValue, { color: colors.text.primary }]}>
            {formatPct(factor.win_rate)}
          </Text>
        </View>
        <View style={s.metricItem}>
          <Text style={[s.metricLabel, { color: colors.text.muted }]}>Straight Sets</Text>
          <Text style={[s.metricValue, { color: colors.text.primary }]}>
            {formatPct(factor.straight_sets_rate)}
          </Text>
        </View>
        <View style={s.metricItem}>
          <Text style={[s.metricLabel, { color: colors.text.muted }]}>Tiebreak %</Text>
          <Text style={[s.metricValue, { color: colors.text.primary }]}>
            {formatPct(factor.tiebreak_rate)}
          </Text>
        </View>
      </View>

      {factor.surface_record && (
        <View style={[s.surfaceBadge, { backgroundColor: colors.surface.cardSoft }]}>
          <Text style={[s.surfaceText, { color: colors.text.secondary }]}>
            Surface: {factor.surface_record.matches} matches, {formatPct(factor.surface_record.win_rate)} win rate
          </Text>
        </View>
      )}

      {factor.recent_results.length > 0 && (
        <Text style={[s.recentResults, { color: colors.text.muted }]}>
          Recent: {factor.recent_results.join(", ")}
        </Text>
      )}
    </View>
  );
}

// ─── Main Screen ─────────────────────────────────

export default function AtpMatchPredictorScreen() {
  const { colors } = useTheme();
  const [searchA, setSearchA] = useState("");
  const [searchB, setSearchB] = useState("");
  const [playerA, setPlayerA] = useState<SearchItem | null>(null);
  const [playerB, setPlayerB] = useState<SearchItem | null>(null);
  const [surface, setSurface] = useState("");

  const { data: playersA } = useAtpPlayers({ search: searchA });
  const { data: playersB } = useAtpPlayers({ search: searchB });

  const itemsA = useMemo(
    () =>
      (playersA?.data || []).map((p) => ({
        id: p.id,
        label: p.full_name || `${p.first_name ?? ""} ${p.last_name ?? ""}`,
        subLabel: p.country ?? "",
      })),
    [playersA],
  );

  const itemsB = useMemo(
    () =>
      (playersB?.data || []).map((p) => ({
        id: p.id,
        label: p.full_name || `${p.first_name ?? ""} ${p.last_name ?? ""}`,
        subLabel: p.country ?? "",
      })),
    [playersB],
  );

  useEffect(() => {
    if (!playerA && itemsA.length > 0) setPlayerA(itemsA[0]);
  }, [itemsA, playerA]);

  useEffect(() => {
    if (!playerB && itemsB.length > 0) {
      const candidate = playerA
        ? itemsB.find((i) => i.id !== playerA.id) ?? null
        : itemsB[0];
      if (candidate) setPlayerB(candidate);
    }
  }, [itemsB, playerB, playerA]);

  const ready =
    !!playerA && !!playerB && playerA.id !== playerB.id;

  const { data, loading, error } = useAtpQuery<MatchPredictorResponse>(
    "/atp/analytics/match-predictor",
    {
      player_id: playerA?.id,
      opponent_id: playerB?.id,
      surface: surface.trim() || undefined,
    },
    ready,
  );

  return (
    <>
      <Stack.Screen
        options={{
          title: "Match Predictor",
          headerStyle: { backgroundColor: colors.surface.screen },
          headerTintColor: colors.text.primary,
        }}
      />
      <ScrollView
        style={{ flex: 1, backgroundColor: colors.surface.screen }}
        contentContainerStyle={s.container}
      >
        <SearchPicker
          title="Player A"
          placeholder="Search player..."
          query={searchA}
          onQueryChange={setSearchA}
          items={itemsA}
          selectedId={playerA?.id}
          onSelect={setPlayerA}
        />
        <SearchPicker
          title="Player B"
          placeholder="Search player..."
          query={searchB}
          onQueryChange={setSearchB}
          items={itemsB}
          selectedId={playerB?.id}
          onSelect={setPlayerB}
        />

        <Text style={{ color: colors.text.primary, fontWeight: "700" }}>
          Surface (optional)
        </Text>
        <TextInput
          value={surface}
          onChangeText={setSurface}
          style={{
            marginTop: 6,
            marginBottom: 12,
            borderWidth: 1,
            borderColor: colors.border.subtle,
            borderRadius: 10,
            paddingHorizontal: 12,
            paddingVertical: 10,
            backgroundColor: colors.surface.card,
            color: colors.text.primary,
          }}
          placeholder="Hard, Clay, Grass"
          placeholderTextColor={colors.text.muted}
        />

        {loading && (
          <View style={s.centered}>
            <ActivityIndicator color={colors.accent.primary} />
            <Text style={[s.loadingText, { color: colors.text.muted }]}>
              Computing prediction...
            </Text>
          </View>
        )}

        {error && (
          <Text style={{ color: colors.accent.danger, marginTop: 12 }}>
            {error}
          </Text>
        )}

        {data?.prediction && (
          <>
            {/* Winner prediction */}
            <View
              style={[
                s.section,
                {
                  backgroundColor: colors.surface.card,
                  borderColor: confidenceColor(data.prediction.confidence, colors),
                  borderWidth: 2,
                },
              ]}
            >
              <View style={s.predictionHeader}>
                <Text style={[s.predictionWinner, { color: colors.text.primary }]}>
                  {data.prediction.predicted_winner_name ?? "Predicted Winner"}
                </Text>
                <View
                  style={[
                    s.confidenceBadge,
                    {
                      backgroundColor:
                        confidenceColor(data.prediction.confidence, colors) + "20",
                    },
                  ]}
                >
                  <Text
                    style={[
                      s.confidenceText,
                      {
                        color: confidenceColor(data.prediction.confidence, colors),
                      },
                    ]}
                  >
                    {data.prediction.confidence} Confidence
                  </Text>
                </View>
              </View>

              <ProbBar
                prob={data.prediction.win_probability}
                label={data.player_name ?? "Player A"}
                color={colors.accent.success}
              />
              <ProbBar
                prob={data.prediction.loss_probability}
                label={data.opponent_name ?? "Player B"}
                color={colors.accent.danger}
              />

              <View style={s.tiebreakRow}>
                <Text style={[s.metricLabel, { color: colors.text.muted }]}>
                  Tiebreak Probability
                </Text>
                <Text style={[s.metricValue, { color: colors.accent.warning }]}>
                  {formatPct(data.prediction.tiebreak_probability)}
                </Text>
              </View>
            </View>

            {/* Set scores */}
            <SetScoreCard scores={data.prediction.set_scores} />

            {/* Insights */}
            {data.insights.length > 0 && (
              <View
                style={[
                  s.section,
                  {
                    backgroundColor: colors.surface.card,
                    borderColor: colors.border.subtle,
                  },
                ]}
              >
                <Text style={[s.sectionTitle, { color: colors.text.primary }]}>
                  Key Insights
                </Text>
                {data.insights.map((insight, i) => (
                  <Text
                    key={i}
                    style={[s.insightText, { color: colors.text.secondary }]}
                  >
                    {insight}
                  </Text>
                ))}
              </View>
            )}

            {/* H2H */}
            {data.factors.head_to_head.starts > 0 && (
              <View
                style={[
                  s.section,
                  {
                    backgroundColor: colors.surface.card,
                    borderColor: colors.border.subtle,
                  },
                ]}
              >
                <Text style={[s.sectionTitle, { color: colors.text.primary }]}>
                  Head to Head
                </Text>
                <View style={s.metricsGrid}>
                  <View style={s.metricItem}>
                    <Text style={[s.metricLabel, { color: colors.text.muted }]}>Matches</Text>
                    <Text style={[s.metricValue, { color: colors.text.primary }]}>
                      {data.factors.head_to_head.starts}
                    </Text>
                  </View>
                  <View style={s.metricItem}>
                    <Text style={[s.metricLabel, { color: colors.text.muted }]}>Record</Text>
                    <Text style={[s.metricValue, { color: colors.text.primary }]}>
                      {data.factors.head_to_head.wins}-{data.factors.head_to_head.losses}
                    </Text>
                  </View>
                  <View style={s.metricItem}>
                    <Text style={[s.metricLabel, { color: colors.text.muted }]}>Win Rate</Text>
                    <Text style={[s.metricValue, { color: colors.text.primary }]}>
                      {formatPct(data.factors.head_to_head.win_rate)}
                    </Text>
                  </View>
                </View>
              </View>
            )}

            {/* Player factors */}
            <FactorCard
              factor={data.factors.player}
              isWinner={data.prediction.predicted_winner_id === data.factors.player.player_id}
            />
            <FactorCard
              factor={data.factors.opponent}
              isWinner={data.prediction.predicted_winner_id === data.factors.opponent.player_id}
            />
          </>
        )}
      </ScrollView>
    </>
  );
}

// ─── Styles ──────────────────────────────────────

const s = StyleSheet.create({
  container: {
    padding: 16,
    paddingBottom: 40,
    gap: 12,
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

  section: {
    padding: 12,
    borderRadius: 16,
    borderWidth: 1,
    gap: 10,
  },
  sectionTitle: {
    fontSize: 14,
    fontWeight: "800",
  },

  // Prediction
  predictionHeader: {
    flexDirection: "row",
    justifyContent: "space-between",
    alignItems: "center",
  },
  predictionWinner: {
    fontSize: 18,
    fontWeight: "900",
  },
  confidenceBadge: {
    paddingHorizontal: 10,
    paddingVertical: 4,
    borderRadius: 8,
  },
  confidenceText: {
    fontSize: 11,
    fontWeight: "700",
  },

  // Prob bar
  probRow: {
    flexDirection: "row",
    alignItems: "center",
    gap: 8,
  },
  probLabel: {
    fontSize: 11,
    fontWeight: "600",
    width: 80,
  },
  barTrack: {
    height: 8,
    borderRadius: 4,
    flex: 1,
    overflow: "hidden",
  },
  barFill: {
    height: "100%",
    borderRadius: 4,
  },
  probValue: {
    fontSize: 13,
    fontWeight: "800",
    width: 40,
    textAlign: "right",
  },

  tiebreakRow: {
    flexDirection: "row",
    justifyContent: "space-between",
    alignItems: "center",
    marginTop: 4,
  },

  // Set scores
  setRow: {
    flexDirection: "row",
    alignItems: "center",
    gap: 8,
  },
  setScore: {
    fontSize: 13,
    fontWeight: "700",
    width: 32,
  },
  setPct: {
    fontSize: 12,
    fontWeight: "600",
    width: 36,
    textAlign: "right",
  },

  // Factor card
  factorHeader: {
    flexDirection: "row",
    justifyContent: "space-between",
    alignItems: "center",
  },
  factorName: {
    fontSize: 16,
    fontWeight: "800",
  },
  rankBadge: {
    fontSize: 14,
    fontWeight: "800",
  },

  metricsGrid: {
    flexDirection: "row",
    flexWrap: "wrap",
    gap: 8,
  },
  metricItem: {
    minWidth: 70,
    flex: 1,
  },
  metricLabel: {
    fontSize: 10,
    fontWeight: "600",
  },
  metricValue: {
    fontSize: 14,
    fontWeight: "700",
    marginTop: 2,
  },

  surfaceBadge: {
    paddingHorizontal: 10,
    paddingVertical: 6,
    borderRadius: 8,
  },
  surfaceText: {
    fontSize: 11,
    fontWeight: "600",
  },

  recentResults: {
    fontSize: 11,
    fontWeight: "500",
  },

  insightText: {
    fontSize: 12,
    fontWeight: "500",
    lineHeight: 17,
    paddingLeft: 8,
  },
});
