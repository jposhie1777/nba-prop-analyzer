import {
  ActivityIndicator,
  Pressable,
  ScrollView,
  StyleSheet,
  Text,
  View,
} from "react-native";
import { useEffect, useMemo, useState } from "react";
import { useTheme } from "@/store/useTheme";
import { useAtpTournamentBracket } from "@/hooks/atp/useAtpTournamentBracket";
import { useAtpActiveTournaments } from "@/hooks/atp/useAtpActiveTournaments";
import type { ActiveTournament } from "@/hooks/atp/useAtpActiveTournaments";
import { useAtpQuery } from "@/hooks/atp/useAtpQuery";
import type {
  AtpBracketMatch,
  AtpBracketRound,
} from "@/hooks/atp/useAtpTournamentBracket";
import type { AtpCompareResponse } from "@/types/atp";

/* ───── helpers ───── */

function formatDateRange(start?: string | null, end?: string | null) {
  if (!start && !end) return "Dates TBD";
  const startDate = start ? new Date(start) : null;
  const endDate = end ? new Date(end) : null;
  const fmt = (v: Date | null) =>
    v
      ? v.toLocaleDateString(undefined, {
          month: "short",
          day: "numeric",
          year: "numeric",
        })
      : "TBD";
  if (startDate && endDate) {
    if (startDate.getMonth() === endDate.getMonth()) {
      return `${startDate.toLocaleDateString(undefined, {
        month: "short",
      })} ${startDate.getDate()} - ${endDate.getDate()}, ${endDate.getFullYear()}`;
    } else {
      return `${fmt(startDate)} - ${fmt(endDate)}`;
    }
  }
  return `${fmt(startDate)} - ${fmt(endDate)}`;
}

function formatMatchDay(value?: string | null) {
  if (!value) return "TBD";
  const d = new Date(value);
  if (Number.isNaN(d.getTime())) return "TBD";
  return d.toLocaleDateString(undefined, {
    day: "numeric",
  });
}

function formatMatchTime(value?: string | null) {
  if (!value) return "TBD";
  const d = new Date(value);
  if (Number.isNaN(d.getTime())) return "TBD";

  if (isToday(value)) {
    return d.toLocaleString(undefined, {
      hour: "numeric",
      minute: "2-digit",
    });
  } else {
    return d.toLocaleString(undefined, {
      month: "short",
      day: "numeric",
    });
  }
}

function formatMatchMeta(match: AtpBracketMatch) {
  return formatMatchTime(match.scheduled_at);
}

function isCompleted(m: AtpBracketMatch) {
  const status = (m.status || "").toString().toLowerCase();
  return (
    status === "f" ||
    status === "finished" ||
    status === "final" ||
    status === "complete" ||
    status === "completed"
  );
}

function isQualifyingRound(r: AtpBracketRound) {
  const n = r.name.toLowerCase();
  return (
    n.includes("qualifying") ||
    n.includes("qualification") ||
    (n.startsWith("q") && /^q\d$/.test(n))
  );
}

function normalizeRoundName(name: string) {
  const trimmed = name.trim();
  const upper = trimmed.toUpperCase();
  if (upper === "R32" || trimmed.toLowerCase() === "round of 32") {
    return "Round of 32";
  }
  return trimmed;
}

function matchKey(match: AtpBracketMatch) {
  return match.id != null ? `id:${match.id}` : `p:${match.player1}-${match.player2}`;
}

function isToday(value?: string | null) {
  if (!value) return false;
  const d = new Date(value);
  if (Number.isNaN(d.getTime())) return false;
  const now = new Date();
  return (
    d.getFullYear() === now.getFullYear() &&
    d.getMonth() === now.getMonth() &&
    d.getDate() === now.getDate()
  );
}

const fmtPct = (v?: number | null) =>
  v == null ? "\u2014" : `${(v * 100).toFixed(1)}%`;
const fmtNum = (v?: number | null) => (v == null ? "\u2014" : v.toFixed(2));

function tournamentShortName(t: ActiveTournament): string {
  return t.city || t.name || "Tournament";
}

function surfaceBadge(surface?: string | null): string {
  if (!surface) return "";
  const s = surface.toLowerCase();
  if (s.includes("hard")) return "Hard";
  if (s.includes("clay")) return "Clay";
  if (s.includes("grass")) return "Grass";
  if (s.includes("carpet")) return "Carpet";
  return surface;
}

type Tab = "bracket" | "today";

/* ───── Bracket tab components ───── */

function MatchCard({
  match,
  colors,
}: {
  match: AtpBracketMatch;
  colors: any;
}) {
  const hasScore = Boolean(match.score);
  const finished = isCompleted(match) || hasScore;
  const winner = match.winner;
  const p1Won = finished && winner === match.player1;
  const p2Won = finished && winner === match.player2;
  const showOutcome = finished && Boolean(winner);

  return (
    <View
      style={[
        s.matchCard,
        {
          backgroundColor: colors.surface.elevated,
          borderColor: finished ? "rgba(34,197,94,0.25)" : colors.border.subtle,
        },
      ]}
    >
      <View style={s.playerRow}>
        <Text
          style={[
            s.playerText,
            {
              color: p1Won
                ? colors.accent.success
                : finished && !p1Won && winner
                  ? colors.text.muted
                  : colors.text.primary,
              fontWeight: p1Won ? "800" : "600",
            },
          ]}
          numberOfLines={1}
        >
          {match.player1}
        </Text>
        {showOutcome && (
          <Text
            style={[
              s.setScores,
              {
                color: p1Won ? colors.accent.success : colors.text.muted,
                fontWeight: p1Won ? "700" : "400",
              },
            ]}
          >
            {p1Won ? "W" : "L"}
          </Text>
        )}
      </View>
      <View style={[s.matchDivider, { backgroundColor: colors.border.subtle }]} />
      <View style={s.playerRow}>
        <Text
          style={[
            s.playerText,
            {
              color: p2Won
                ? colors.accent.success
                : finished && !p2Won && winner
                  ? colors.text.muted
                  : colors.text.primary,
              fontWeight: p2Won ? "800" : "600",
            },
          ]}
          numberOfLines={1}
        >
          {match.player2}
        </Text>
        {showOutcome && (
          <Text
            style={[
              s.setScores,
              {
                color: p2Won ? colors.accent.success : colors.text.muted,
                fontWeight: p2Won ? "700" : "400",
              },
            ]}
          >
            {p2Won ? "W" : "L"}
          </Text>
        )}
      </View>
      {finished ? (
        <>
          <Text
            style={[
              hasScore ? s.scoreText : s.matchMeta,
              { color: hasScore ? colors.text.secondary : colors.text.muted },
            ]}
          >
            {hasScore ? match.score : "Final"}
          </Text>
          <Text style={[s.matchMeta, { color: colors.text.muted }]}>
            {formatMatchMeta(match)}
          </Text>
        </>
      ) : (
        <Text style={[s.matchMeta, { color: colors.text.muted }]}>
          {formatMatchMeta(match)}
        </Text>
      )}
    </View>
  );
}

function RoundColumn({
  round,
  colors,
}: {
  round: AtpBracketRound;
  colors: any;
}) {
  return (
    <View style={s.roundColumn}>
      <View style={[s.roundHeader, { backgroundColor: colors.surface.cardSoft }]}>
        <Text style={[s.roundTitle, { color: colors.text.primary }]}>
          {round.name}
        </Text>
        <Text style={[s.roundCount, { color: colors.text.muted }]}>
          {round.matches.length} {round.matches.length === 1 ? "match" : "matches"}
        </Text>
      </View>
      {round.matches.length === 0 ? (
        <Text style={[s.emptyText, { color: colors.text.muted }]}>
          Matches TBD
        </Text>
      ) : (
        round.matches.map((m) => (
          <MatchCard
            key={`${round.name}-${m.id ?? m.player1}-${m.player2}`}
            match={m}
            colors={colors}
          />
        ))
      )}
    </View>
  );
}

/* ───── Today's matches: per-match analysis card ───── */

function MatchAnalysisCard({
  match,
  surface,
  colors,
}: {
  match: AtpBracketMatch;
  surface?: string | null;
  colors: any;
}) {
  const p1Id = match.player1_id;
  const p2Id = match.player2_id;
  const hasBothIds = p1Id != null && p2Id != null && p1Id !== p2Id;

  const { data, loading, error } = useAtpQuery<AtpCompareResponse>(
    "/atp/analytics/compare",
    {
      player_ids: hasBothIds ? [p1Id!, p2Id!] : [],
      surface: surface?.toLowerCase() || undefined,
    },
    hasBothIds
  );

  const rec = data?.recommendation;
  const players = data?.players ?? [];
  const p1Data = players.find((p) => p.player_id === p1Id);
  const p2Data = players.find((p) => p.player_id === p2Id);

  const pickName =
    rec?.player_id === p1Id
      ? match.player1
      : rec?.player_id === p2Id
        ? match.player2
        : null;

  return (
    <View
      style={[
        s.analysisCard,
        { backgroundColor: colors.surface.card, borderColor: colors.border.subtle },
      ]}
    >
      {/* Match header */}
      <View style={s.analysisHeader}>
        <Text style={[s.analysisRound, { color: colors.text.muted }]}>
          {match.round} {"\u2022"} {formatMatchMeta(match)}
        </Text>
      </View>

      {/* Players matchup */}
      <View style={s.matchupRow}>
        <View style={s.matchupPlayer}>
          <Text
            style={[s.matchupName, { color: colors.text.primary }]}
            numberOfLines={1}
          >
            {match.player1}
          </Text>
          {p1Data && (
            <Text style={[s.matchupScore, { color: colors.text.secondary }]}>
              Score: {fmtNum(p1Data.score)}
            </Text>
          )}
        </View>
        <Text style={[s.vsText, { color: colors.text.muted }]}>vs</Text>
        <View style={[s.matchupPlayer, { alignItems: "flex-end" }]}>
          <Text
            style={[s.matchupName, { color: colors.text.primary, textAlign: "right" }]}
            numberOfLines={1}
          >
            {match.player2}
          </Text>
          {p2Data && (
            <Text style={[s.matchupScore, { color: colors.text.secondary }]}>
              Score: {fmtNum(p2Data.score)}
            </Text>
          )}
        </View>
      </View>

      {/* Analytics */}
      {loading ? (
        <View style={s.analysisLoading}>
          <ActivityIndicator size="small" color={colors.accent.primary} />
          <Text style={[s.analysisLoadingText, { color: colors.text.muted }]}>
            Analyzing matchup...
          </Text>
        </View>
      ) : error ? (
        <Text style={[s.analysisError, { color: colors.text.muted }]}>
          Analysis unavailable
        </Text>
      ) : !hasBothIds ? (
        <Text style={[s.analysisError, { color: colors.text.muted }]}>
          Player data not available for analysis
        </Text>
      ) : data ? (
        <View style={s.analyticsSection}>
          {/* Recommendation */}
          {rec && pickName && (
            <View
              style={[
                s.recCard,
                { backgroundColor: colors.glow.success, borderColor: "rgba(34,197,94,0.3)" },
              ]}
            >
              <Text style={[s.recLabel, { color: colors.accent.success }]}>
                PICK: {pickName}
              </Text>
              <Text style={[s.recEdge, { color: colors.text.primary }]}>
                Edge: {fmtNum(rec.edge)}
              </Text>
              {rec.reasons.length > 0 && (
                <Text style={[s.recReasons, { color: colors.text.secondary }]}>
                  {rec.reasons.join(" \u2022 ")}
                </Text>
              )}
            </View>
          )}

          {/* Head-to-head metrics */}
          <View style={s.metricsGrid}>
            {/* Column headers */}
            <View style={s.metricsHeaderRow}>
              <Text style={[s.metricsHeaderLabel, { color: colors.text.muted }]} />
              <Text
                style={[s.metricsHeaderVal, { color: colors.text.muted }]}
                numberOfLines={1}
              >
                {match.player1.split(" ").pop()}
              </Text>
              <Text
                style={[s.metricsHeaderVal, { color: colors.text.muted, textAlign: "right" }]}
                numberOfLines={1}
              >
                {match.player2.split(" ").pop()}
              </Text>
            </View>

            <View style={[s.metricsGridDivider, { backgroundColor: colors.border.subtle }]} />

            {/* Form */}
            <MetricRow
              label="Form"
              v1={fmtNum(p1Data?.metrics.form_score)}
              v2={fmtNum(p2Data?.metrics.form_score)}
              better={compareBetter(p1Data?.metrics.form_score, p2Data?.metrics.form_score)}
              colors={colors}
            />
            {/* Win rate */}
            <MetricRow
              label="Win Rate"
              v1={fmtPct(p1Data?.metrics.recent_win_rate)}
              v2={fmtPct(p2Data?.metrics.recent_win_rate)}
              better={compareBetter(p1Data?.metrics.recent_win_rate, p2Data?.metrics.recent_win_rate)}
              colors={colors}
            />
            {/* Surface win rate */}
            <MetricRow
              label={`${surface || "Surface"} %`}
              v1={fmtPct(p1Data?.metrics.surface_win_rate)}
              v2={fmtPct(p2Data?.metrics.surface_win_rate)}
              better={compareBetter(p1Data?.metrics.surface_win_rate, p2Data?.metrics.surface_win_rate)}
              colors={colors}
            />
            {/* Ranking */}
            <MetricRow
              label="Ranking"
              v1={p1Data?.metrics.ranking != null ? `#${p1Data.metrics.ranking}` : "\u2014"}
              v2={p2Data?.metrics.ranking != null ? `#${p2Data.metrics.ranking}` : "\u2014"}
              better={compareRanking(p1Data?.metrics.ranking, p2Data?.metrics.ranking)}
              colors={colors}
            />
          </View>

          {/* H2H summary */}
          {data.head_to_head && data.head_to_head.starts > 0 && (
            <View style={[s.h2hRow, { backgroundColor: colors.surface.cardSoft }]}>
              <Text style={[s.h2hLabel, { color: colors.text.muted }]}>
                Head-to-Head
              </Text>
              <Text style={[s.h2hValue, { color: colors.text.primary }]}>
                {data.head_to_head.wins}-{data.head_to_head.losses} ({fmtPct(data.head_to_head.win_rate)})
              </Text>
            </View>
          )}
        </View>
      ) : null}
    </View>
  );
}

function compareBetter(
  v1?: number | null,
  v2?: number | null
): "p1" | "p2" | "even" {
  if (v1 == null || v2 == null) return "even";
  if (v1 > v2) return "p1";
  if (v2 > v1) return "p2";
  return "even";
}

function compareRanking(
  v1?: number | null,
  v2?: number | null
): "p1" | "p2" | "even" {
  if (v1 == null || v2 == null) return "even";
  if (v1 < v2) return "p1"; // lower ranking = better
  if (v2 < v1) return "p2";
  return "even";
}

function MetricRow({
  label,
  v1,
  v2,
  better,
  colors,
}: {
  label: string;
  v1: string;
  v2: string;
  better: "p1" | "p2" | "even";
  colors: any;
}) {
  return (
    <View style={s.metricRow}>
      <Text style={[s.metricLabel, { color: colors.text.muted }]}>{label}</Text>
      <Text
        style={[
          s.metricVal,
          {
            color:
              better === "p1" ? colors.accent.success : colors.text.primary,
            fontWeight: better === "p1" ? "700" : "500",
          },
        ]}
      >
        {v1}
      </Text>
      <Text
        style={[
          s.metricVal,
          {
            color:
              better === "p2" ? colors.accent.success : colors.text.primary,
            fontWeight: better === "p2" ? "700" : "500",
            textAlign: "right",
          },
        ]}
      >
        {v2}
      </Text>
    </View>
  );
}

/* ───── Per-tournament bracket view (keyed by tournament id) ───── */

function TournamentBracketView({
  tournamentId,
  colors,
}: {
  tournamentId: number;
  colors: any;
}) {
  const [tab, setTab] = useState<Tab>("bracket");

  const { data, loading, error } = useAtpTournamentBracket({
    tournamentId,
    upcomingLimit: 50,
  });

  const header = useMemo(() => {
    if (!data?.tournament) {
      return { name: "ATP Tournament Bracket", surface: null as string | null, dates: "Dates TBD" };
    }
    return {
      name: data.tournament.name,
      surface: data.tournament.surface ?? null,
      dates: formatDateRange(data.tournament.start_date, data.tournament.end_date),
    };
  }, [data]);

  const { qualifyingRounds, mainDrawRounds, stats } = useMemo(() => {
    const rounds = data?.bracket.rounds ?? [];
    const qualifying: AtpBracketRound[] = [];
    const mainDraw: AtpBracketRound[] = [];
    const upcomingMatches = data?.upcoming_matches ?? [];
    const upcomingTimes = new Map<string, string>();
    for (const match of upcomingMatches) {
      if (match.scheduled_at) {
        upcomingTimes.set(matchKey(match), match.scheduled_at);
      }
    }
    for (const round of rounds) {
      const normalizedName = normalizeRoundName(round.name);
      const target = isQualifyingRound(round) ? qualifying : mainDraw;
      const matches = round.matches.map((match) => {
        if (match.scheduled_at) return match;
        const scheduledAt = upcomingTimes.get(matchKey(match));
        return scheduledAt ? { ...match, scheduled_at: scheduledAt } : match;
      });
      const existing = target.find((item) => item.name === normalizedName);
      if (existing) {
        existing.matches.push(...matches);
        if (existing.order == null && round.order != null) {
          existing.order = round.order;
        }
      } else {
        target.push({
          name: normalizedName,
          order: round.order,
          matches,
        });
      }
    }
    let total = 0;
    let completed = 0;
    for (const round of rounds) {
      for (const m of round.matches) {
        total++;
        if (isCompleted(m)) completed++;
      }
    }
    return {
      qualifyingRounds: qualifying,
      mainDrawRounds: mainDraw,
      stats: { total, completed },
    };
  }, [data]);

  const todayMatches = useMemo(() => {
    const all: AtpBracketMatch[] = [];
    for (const round of data?.bracket.rounds ?? []) {
      for (const m of round.matches) {
        if (!isCompleted(m) && isToday(m.scheduled_at)) {
          all.push(m);
        }
      }
    }
    for (const m of data?.upcoming_matches ?? []) {
      if (isToday(m.scheduled_at) && !all.some((e) => e.id === m.id)) {
        all.push(m);
      }
    }
    if (all.length === 0) {
      for (const m of data?.upcoming_matches ?? []) {
        if (!isCompleted(m)) all.push(m);
      }
    }
    return all;
  }, [data]);

  const surfaceLabel = header.surface ? `${header.surface} Court` : "Surface TBD";

  return (
    <>
      {/* Tournament Header */}
      <View
        style={[
          s.headerCard,
          { backgroundColor: colors.surface.card, borderColor: colors.border.subtle },
        ]}
      >
        <Text style={[s.bracketTitle, { color: colors.text.primary }]}>
          {header.name}
        </Text>
        <Text style={[s.bracketMeta, { color: colors.text.muted }]}>
          {surfaceLabel} {"\u2022"} {header.dates}
        </Text>
        {!loading && !error && stats.total > 0 && (
          <View style={s.progressRow}>
            <View style={[s.progressBar, { backgroundColor: colors.surface.elevated }]}>
              <View
                style={[
                  s.progressFill,
                  {
                    backgroundColor: colors.accent.success,
                    width: `${Math.round((stats.completed / stats.total) * 100)}%` as any,
                  },
                ]}
              />
            </View>
            <Text style={[s.progressText, { color: colors.text.muted }]}>
              {stats.completed}/{stats.total} completed
            </Text>
          </View>
        )}
      </View>

      {/* Segmented control */}
      <View style={s.tabRow}>
        <Pressable
          onPress={() => setTab("bracket")}
          style={[
            s.tabBtn,
            {
              borderColor:
                tab === "bracket" ? colors.accent.primary : colors.border.subtle,
              backgroundColor:
                tab === "bracket" ? colors.state.selected : colors.surface.card,
            },
          ]}
        >
          <Text
            style={[
              s.tabLabel,
              {
                color:
                  tab === "bracket"
                    ? colors.accent.primary
                    : colors.text.secondary,
              },
            ]}
          >
            Bracket
          </Text>
        </Pressable>
        <Pressable
          onPress={() => setTab("today")}
          style={[
            s.tabBtn,
            {
              borderColor:
                tab === "today" ? colors.accent.primary : colors.border.subtle,
              backgroundColor:
                tab === "today" ? colors.state.selected : colors.surface.card,
            },
          ]}
        >
          <Text
            style={[
              s.tabLabel,
              {
                color:
                  tab === "today"
                    ? colors.accent.primary
                    : colors.text.secondary,
              },
            ]}
          >
            Today's Matches
          </Text>
          {!loading && todayMatches.length > 0 && (
            <View style={[s.badge, { backgroundColor: colors.accent.primary }]}>
              <Text style={s.badgeText}>{todayMatches.length}</Text>
            </View>
          )}
        </Pressable>
      </View>

      {/* Content */}
      {loading ? (
        <View style={s.loadingContainer}>
          <ActivityIndicator color={colors.accent.primary} />
          <Text style={[s.statusText, { color: colors.text.muted }]}>
            Loading bracket...
          </Text>
        </View>
      ) : error ? (
        <Text style={[s.statusText, { color: colors.text.primary }]}>
          {error}
        </Text>
      ) : tab === "bracket" ? (
        <>
          {mainDrawRounds.length > 0 && (
            <View style={s.section}>
              <Text style={[s.sectionTitle, { color: colors.text.primary }]}>
                Main Draw
              </Text>
              <ScrollView
                horizontal
                showsHorizontalScrollIndicator={false}
                contentContainerStyle={s.roundsRow}
              >
                {mainDrawRounds.map((r) => (
                  <RoundColumn key={r.name} round={r} colors={colors} />
                ))}
              </ScrollView>
            </View>
          )}
          {qualifyingRounds.length > 0 && (
            <View style={s.section}>
              <Text style={[s.sectionTitle, { color: colors.text.muted }]}>
                Qualifying
              </Text>
              <ScrollView
                horizontal
                showsHorizontalScrollIndicator={false}
                contentContainerStyle={s.roundsRow}
              >
                {qualifyingRounds.map((r) => (
                  <RoundColumn key={r.name} round={r} colors={colors} />
                ))}
              </ScrollView>
            </View>
          )}
        </>
      ) : (
        <View style={s.section}>
          {todayMatches.length === 0 ? (
            <View style={s.emptyState}>
              <Text style={[s.emptyStateTitle, { color: colors.text.primary }]}>
                No upcoming matches
              </Text>
              <Text style={[s.emptyStateBody, { color: colors.text.muted }]}>
                Check back when matches are scheduled.
              </Text>
            </View>
          ) : (
            todayMatches.map((m) => (
              <MatchAnalysisCard
                key={`analysis-${m.id ?? m.player1}-${m.player2}`}
                match={m}
                surface={header.surface}
                colors={colors}
              />
            ))
          )}
        </View>
      )}
    </>
  );
}

/* ───── Main screen ───── */

export default function AtpBracketScreen() {
  const { colors } = useTheme();
  const [selectedId, setSelectedId] = useState<number | null>(null);

  const {
    data: tournamentsData,
    loading: tournamentsLoading,
    error: tournamentsError,
  } = useAtpActiveTournaments();

  const tournaments = tournamentsData?.tournaments ?? [];

  // Auto-select first tournament when data arrives
  useEffect(() => {.
    if (tournaments.length > 0 && selectedId === null) {
      setSelectedId(tournaments[0].id);
    }
  }, [tournaments, selectedId]);

  // If the selected tournament is no longer in the list, reset
  useEffect(() => {
    if (
      selectedId !== null &&
      tournaments.length > 0 &&
      !tournaments.some((t) => t.id === selectedId)
    ) {
      setSelectedId(tournaments[0].id);
    }
  }, [tournaments, selectedId]);

  const selectedTournament = tournaments.find((t) => t.id === selectedId) ?? null;

  return (
    <ScrollView
      style={{ flex: 1, backgroundColor: colors.surface.screen }}
      contentContainerStyle={s.container}
    >
      {/* Tournament selector */}
      {tournamentsLoading ? (
        <View style={s.loadingContainer}>
          <ActivityIndicator color={colors.accent.primary} />
          <Text style={[s.statusText, { color: colors.text.muted }]}>
            Finding active tournaments...
          </Text>
        </View>
      ) : tournamentsError ? (
        <Text style={[s.statusText, { color: colors.text.primary }]}>
          {tournamentsError}
        </Text>
      ) : tournaments.length === 0 ? (
        <View style={s.emptyState}>
          <Text style={[s.emptyStateTitle, { color: colors.text.primary }]}>
            No active tournaments
          </Text>
          <Text style={[s.emptyStateBody, { color: colors.text.muted }]}>
            Check back when ATP tournaments are running.
          </Text>
        </View>
      ) : (
        <>
          {/* Tournament pills */}
          <ScrollView
            horizontal
            showsHorizontalScrollIndicator={false}
            contentContainerStyle={s.tournamentPillsRow}
          >
            {tournaments.map((t) => {
              const active = t.id === selectedId;
              const surface = surfaceBadge(t.surface);
              return (
                <Pressable
                  key={t.id}
                  onPress={() => setSelectedId(t.id)}
                  style={[
                    s.tournamentPill,
                    {
                      backgroundColor: active
                        ? colors.accent.primary
                        : colors.surface.card,
                      borderColor: active
                        ? colors.accent.primary
                        : colors.border.subtle,
                    },
                  ]}
                >
                  <Text
                    style={[
                      s.tournamentPillName,
                      {
                        color: active ? "#fff" : colors.text.primary,
                      },
                    ]}
                    numberOfLines={1}
                  >
                    {tournamentShortName(t)}
                  </Text>
                  {surface ? (
                    <Text
                      style={[
                        s.tournamentPillSurface,
                        {
                          color: active
                            ? "rgba(255,255,255,0.75)"
                            : colors.text.muted,
                        },
                      ]}
                    >
                      {surface}
                    </Text>
                  ) : null}
                </Pressable>
              );
            })}
          </ScrollView>

          {/* Selected tournament bracket */}
          {selectedId != null && (
            <TournamentBracketView
              key={selectedId}
              tournamentId={selectedId}
              colors={colors}
            />
          )}
        </>
      )}
    </ScrollView>
  );
}

/* ───── Styles ───── */

const s = StyleSheet.create({
  container: {
    padding: 16,
    paddingBottom: 40,
    gap: 16,
  },

  /* Tournament pills */
  tournamentPillsRow: {
    gap: 10,
    paddingVertical: 2,
  },
  tournamentPill: {
    paddingHorizontal: 16,
    paddingVertical: 10,
    borderRadius: 20,
    borderWidth: 1,
    alignItems: "center",
    minWidth: 100,
  },
  tournamentPillName: {
    fontSize: 14,
    fontWeight: "700",
  },
  tournamentPillSurface: {
    fontSize: 10,
    fontWeight: "500",
    marginTop: 2,
  },

  /* Header */
  headerCard: {
    borderRadius: 16,
    borderWidth: StyleSheet.hairlineWidth,
    padding: 16,
    gap: 8,
    alignItems: "center",
  },
  bracketTitle: { fontSize: 20, fontWeight: "800", textAlign: "center" },
  bracketMeta: { fontSize: 13, textAlign: "center" },
  progressRow: {
    flexDirection: "row",
    alignItems: "center",
    gap: 10,
    marginTop: 4,
    width: "100%",
  },
  progressBar: { flex: 1, height: 4, borderRadius: 2, overflow: "hidden" },
  progressFill: { height: "100%", borderRadius: 2 },
  progressText: { fontSize: 11 },

  /* Segmented control */
  tabRow: { flexDirection: "row", gap: 10 },
  tabBtn: {
    flex: 1,
    flexDirection: "row",
    alignItems: "center",
    justifyContent: "center",
    paddingVertical: 10,
    borderRadius: 12,
    borderWidth: 1,
    gap: 6,
  },
  tabLabel: { fontSize: 13, fontWeight: "600" },
  badge: {
    minWidth: 20,
    height: 20,
    borderRadius: 10,
    alignItems: "center",
    justifyContent: "center",
    paddingHorizontal: 6,
  },
  badgeText: { fontSize: 11, fontWeight: "700", color: "#fff" },

  /* Bracket tab */
  section: { gap: 10 },
  sectionTitle: {
    fontSize: 15,
    fontWeight: "700",
    letterSpacing: 0.5,
    textTransform: "uppercase",
  },
  roundsRow: { paddingVertical: 4, gap: 12 },
  roundColumn: { width: 190, gap: 8 },
  roundHeader: { borderRadius: 8, paddingVertical: 6, paddingHorizontal: 10, gap: 2 },
  roundTitle: { fontSize: 13, fontWeight: "700" },
  roundCount: { fontSize: 10 },
  matchCard: { borderRadius: 10, borderWidth: 1, padding: 8, gap: 4 },
  playerRow: {
    flexDirection: "row",
    justifyContent: "space-between",
    alignItems: "center",
    gap: 4,
  },
  playerText: { fontSize: 12, flex: 1 },
  setScores: { fontSize: 11 },
  matchDivider: { height: StyleSheet.hairlineWidth },
  scoreText: { fontSize: 11, fontWeight: "600", marginTop: 2 },
  matchMeta: { fontSize: 10, marginTop: 2 },

  /* Analysis card */
  analysisCard: {
    borderRadius: 14,
    borderWidth: 1,
    padding: 14,
    gap: 12,
  },
  analysisHeader: { gap: 2 },
  analysisRound: { fontSize: 11, fontWeight: "600", textTransform: "uppercase", letterSpacing: 0.3 },

  matchupRow: {
    flexDirection: "row",
    alignItems: "center",
    gap: 8,
  },
  matchupPlayer: { flex: 1, gap: 2 },
  matchupName: { fontSize: 15, fontWeight: "700" },
  matchupScore: { fontSize: 11 },
  vsText: { fontSize: 12, fontWeight: "600" },

  analyticsSection: { gap: 10 },

  /* Recommendation */
  recCard: {
    borderRadius: 10,
    borderWidth: 1,
    padding: 10,
    gap: 4,
  },
  recLabel: { fontSize: 13, fontWeight: "800", letterSpacing: 0.3 },
  recEdge: { fontSize: 12, fontWeight: "600" },
  recReasons: { fontSize: 11, lineHeight: 16 },

  /* Metrics grid */
  metricsGrid: { gap: 6 },
  metricsHeaderRow: {
    flexDirection: "row",
    alignItems: "center",
    paddingBottom: 4,
  },
  metricsHeaderLabel: { flex: 1.2, fontSize: 10 },
  metricsHeaderVal: { flex: 1, fontSize: 10, fontWeight: "600" },
  metricsGridDivider: { height: StyleSheet.hairlineWidth },
  metricRow: { flexDirection: "row", alignItems: "center", paddingVertical: 3 },
  metricLabel: { flex: 1.2, fontSize: 12 },
  metricVal: { flex: 1, fontSize: 12 },

  /* H2H */
  h2hRow: {
    flexDirection: "row",
    justifyContent: "space-between",
    alignItems: "center",
    borderRadius: 8,
    padding: 10,
  },
  h2hLabel: { fontSize: 12, fontWeight: "600" },
  h2hValue: { fontSize: 12, fontWeight: "700" },

  /* Loading / empty */
  loadingContainer: { alignItems: "center", gap: 8, padding: 20 },
  statusText: { fontSize: 13, textAlign: "center", padding: 20 },
  emptyText: { fontSize: 12, fontStyle: "italic", padding: 8 },
  emptyState: { alignItems: "center", padding: 32, gap: 6 },
  emptyStateTitle: { fontSize: 15, fontWeight: "700" },
  emptyStateBody: { fontSize: 12, textAlign: "center" },
  analysisLoading: {
    flexDirection: "row",
    alignItems: "center",
    gap: 8,
    padding: 8,
  },
  analysisLoadingText: { fontSize: 12 },
  analysisError: { fontSize: 12, padding: 8 },
});