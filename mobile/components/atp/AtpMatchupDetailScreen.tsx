import { useEffect, useMemo, useState } from "react";
import { ActivityIndicator, Pressable, ScrollView, StyleSheet, Text, View } from "react-native";
import { useLocalSearchParams, useRouter } from "expo-router";
import * as Clipboard from "expo-clipboard";

import { AtpMatchupCard } from "@/components/atp/AtpMatchupCard";
import {
  AtpOddsBoardRow,
  useAtpMatchupDetail,
} from "@/hooks/atp/useAtpMatchups";
import { useAtpBetslip } from "@/store/useAtpBetslip";
import { useAtpBetslipDrawer } from "@/store/useAtpBetslipDrawer";
import { useTheme } from "@/store/useTheme";

type SectionKey =
  | "oddsBoard"
  | "h2hStats"
  | "h2hSummary"
  | "playerStatsAnalysis"
  | "playerHistory"
  | "recentMatches"
  | "bettingInfo"
  | "matchInfo";

function stringifyValue(value: unknown): string {
  if (value == null) return "–";
  if (typeof value === "string") return value;
  if (typeof value === "number" || typeof value === "boolean") return String(value);
  try {
    return JSON.stringify(value);
  } catch {
    return String(value);
  }
}

function formatShortDate(value?: string | null) {
  if (!value) return "Date TBD";
  const date = new Date(value);
  if (Number.isNaN(date.getTime())) return "Date TBD";
  return date.toLocaleDateString(undefined, {
    month: "short",
    day: "numeric",
  });
}

function formatOddsAmerican(value?: number | null): string {
  if (value == null) return "—";
  return value > 0 ? `+${value}` : String(value);
}

function decimalToAmerican(value?: number | null): number | null {
  if (value == null || Number.isNaN(value) || value <= 1) return null;
  if (value >= 2) return Math.round((value - 1) * 100);
  return Math.round(-100 / (value - 1));
}

function toAmericanNumber(row: Pick<AtpOddsBoardRow, "odds_american" | "odds_decimal">): number | null {
  if (row.odds_american != null) return row.odds_american;
  return decimalToAmerican(row.odds_decimal);
}

function formatAmericanOnly(row: Pick<AtpOddsBoardRow, "odds_american" | "odds_decimal">): string {
  const american = toAmericanNumber(row);
  if (american == null) return "—";
  return formatOddsAmerican(american);
}

function marketLabelForRow(row: AtpOddsBoardRow): string {
  const raw = row.market_group ?? row.market;
  const cleaned = raw?.trim();
  return cleaned || "Other";
}

function marketKey(label: string): string {
  return label.trim().toLowerCase();
}

function parseLineNumber(value?: string | null): number | null {
  if (!value) return null;
  const match = String(value).match(/-?\d+(?:\.\d+)?/);
  if (!match) return null;
  const parsed = Number(match[0]);
  return Number.isFinite(parsed) ? parsed : null;
}

function outcomeSideForRow(row: AtpOddsBoardRow): "home" | "away" | null {
  const side = row.outcome_side?.trim().toLowerCase();
  if (side === "home" || side === "away") return side;
  const raw = row.outcome_name?.trim().toLowerCase();
  if (raw === "home" || raw === "o1" || raw === "1") return "home";
  if (raw === "away" || raw === "o2" || raw === "2") return "away";
  return null;
}

function splitDualLineValue(value: string): { home: string; away: string } | null {
  const parts = value.split("/");
  if (parts.length !== 2) return null;
  const homeRaw = parts[0].trim();
  const awayRaw = parts[1].trim();

  const numberRegex = /^[+-]?\d+(?:\.\d+)?/;
  const homeNum = homeRaw.match(numberRegex)?.[0] ?? homeRaw;
  const awayNum = awayRaw.match(numberRegex)?.[0] ?? awayRaw;
  const homeSuffix = homeRaw.replace(numberRegex, "").trim();
  const awaySuffix = awayRaw.replace(numberRegex, "").trim();
  const suffix = awaySuffix || homeSuffix;

  const home = suffix && homeRaw === homeNum ? `${homeNum} ${suffix}` : homeRaw;
  const away = suffix && awayRaw === awayNum ? `${awayNum} ${suffix}` : awayRaw;
  return { home: home.trim(), away: away.trim() };
}

function lineValueForOutcome(row: AtpOddsBoardRow): string | null {
  const raw = row.line_value?.trim();
  if (!raw) return null;
  const split = splitDualLineValue(raw);
  if (!split) return raw;
  const side = outcomeSideForRow(row);
  if (side === "home") return split.home;
  if (side === "away") return split.away;
  return raw;
}

function rawOutcomeLabel(row: AtpOddsBoardRow): string {
  const fromName = row.outcome_name?.trim();
  if (fromName) return fromName;
  const fromSide = row.outcome_side?.trim();
  if (fromSide) return fromSide;
  return "Outcome";
}

function displayOutcomeLabel(row: AtpOddsBoardRow, homePlayer: string, awayPlayer: string): string {
  const raw = rawOutcomeLabel(row).toLowerCase();
  const side = row.outcome_side?.toLowerCase();
  if (side === "home") return homePlayer;
  if (side === "away") return awayPlayer;
  if (raw === "home" || raw === "o1" || raw === "1") return homePlayer;
  if (raw === "away" || raw === "o2" || raw === "2") return awayPlayer;
  return rawOutcomeLabel(row);
}

function isOverOutcome(row: AtpOddsBoardRow): boolean {
  const label = rawOutcomeLabel(row).toLowerCase();
  return label.startsWith("over");
}

function isUnderOutcome(row: AtpOddsBoardRow): boolean {
  const label = rawOutcomeLabel(row).toLowerCase();
  return label.startsWith("under");
}

function hasTotalWord(value: string): boolean {
  return value.toLowerCase().includes("total");
}

function formatRecentOutcome(value?: string | null): string {
  const normalized = (value ?? "").toLowerCase();
  if (normalized === "w") return "W";
  if (normalized === "l") return "L";
  return value ?? "N/A";
}

function buildOddsExport(rows: AtpOddsBoardRow[], homePlayer: string, awayPlayer: string): string {
  const header = [
    "market_group",
    "market",
    "period_id",
    "period_name",
    "line_value",
    "outcome_name",
    "bookie",
    "odds_american",
  ].join("\t");
  const lines = rows.map((row) =>
    [
      row.market_group ?? "",
      row.market ?? "",
      row.period_id ?? "",
      row.period_name ?? "",
      lineValueForOutcome(row) ?? "",
      displayOutcomeLabel(row, homePlayer, awayPlayer),
      row.bookie ?? "",
      formatAmericanOnly(row),
    ].join("\t")
  );
  return [header, ...lines].join("\n");
}

function formatMatchTime(value?: string | null): string | undefined {
  if (!value) return undefined;
  const date = new Date(value);
  if (Number.isNaN(date.getTime())) return undefined;
  return date.toLocaleString();
}

function titleCaseWords(value?: string | null): string {
  if (!value) return "";
  return value
    .split("_")
    .map((token) => token.trim())
    .filter(Boolean)
    .map((token) => token[0].toUpperCase() + token.slice(1).toLowerCase())
    .join(" ");
}

function formatBettingMetric(value: number | null | undefined, label?: string | null): string {
  if (value == null || Number.isNaN(value)) return "—";
  if ((label ?? "").toLowerCase().includes("percentage")) return `${value}%`;
  return `${value}`;
}

function formatDecimal(value?: number | null, digits = 2): string {
  if (value == null || Number.isNaN(value)) return "—";
  return value.toFixed(digits);
}

function formatPercent(value?: number | null): string {
  if (value == null || Number.isNaN(value)) return "—";
  return `${value.toFixed(1)}%`;
}

export function AtpMatchupDetailScreen() {
  const router = useRouter();
  const { colors } = useTheme();
  const params = useLocalSearchParams<{
    matchId?: string;
    homePlayer?: string;
    awayPlayer?: string;
    startTimeUtc?: string;
    tournamentName?: string;
    roundName?: string;
    homeRank?: string;
    awayRank?: string;
    homeHeadshotUrl?: string;
    awayHeadshotUrl?: string;
  }>();
  const matchId = Number(params.matchId);
  const { data, loading, error, refetch } = useAtpMatchupDetail(Number.isFinite(matchId) ? matchId : null);

  const betslipItems = useAtpBetslip((s) => s.items);
  const addToBetslip = useAtpBetslip((s) => s.add);
  const openDrawer = useAtpBetslipDrawer((s) => s.open);

  const [expanded, setExpanded] = useState<Record<SectionKey, boolean>>({
    oddsBoard: true,
    h2hStats: true,
    h2hSummary: true,
    playerStatsAnalysis: true,
    playerHistory: true,
    recentMatches: false,
    bettingInfo: false,
    matchInfo: false,
  });
  const [copyStatus, setCopyStatus] = useState<"idle" | "copied">("idle");
  const [selectedMarket, setSelectedMarket] = useState<string>("all");

  const matchup = data?.matchup ?? {};
  const homePlayer = params.homePlayer ?? matchup.home_team ?? "Player 1";
  const awayPlayer = params.awayPlayer ?? matchup.away_team ?? "Player 2";
  const startTimeUtc = params.startTimeUtc ?? matchup.start_time_utc ?? null;
  const tournamentName = params.tournamentName ?? matchup.tournament_name ?? null;
  const roundName = params.roundName ?? matchup.round_name ?? null;
  const homeRank = params.homeRank ?? matchup.home_rank ?? null;
  const awayRank = params.awayRank ?? matchup.away_rank ?? null;
  const homeHeadshotUrl = params.homeHeadshotUrl ?? matchup.home_headshot_url ?? null;
  const awayHeadshotUrl = params.awayHeadshotUrl ?? matchup.away_headshot_url ?? null;
  const playerHistory = data?.player_match_history;
  const homeHistory = playerHistory?.home ?? null;
  const awayHistory = playerHistory?.away ?? null;
  const playerStatsAnalysis = data?.player_stats_analysis;
  const homeStatsAnalysis = playerStatsAnalysis?.home ?? null;
  const awayStatsAnalysis = playerStatsAnalysis?.away ?? null;

  const marketOptions = useMemo(() => {
    const seen = new Map<string, string>();
    (data?.odds_board ?? []).forEach((row) => {
      const label = marketLabelForRow(row);
      const key = marketKey(label);
      if (!seen.has(key)) seen.set(key, label);
    });
    return Array.from(seen.entries()).map(([key, label]) => ({ key, label }));
  }, [data?.odds_board]);

  const filteredOddsRows = useMemo(() => {
    const rows = data?.odds_board ?? [];
    if (selectedMarket === "all") return rows;
    return rows.filter((row) => marketKey(marketLabelForRow(row)) === selectedMarket);
  }, [data?.odds_board, selectedMarket]);

  const groupedOddsRows = useMemo(() => {
    const marketMap = new Map<string, Map<string, AtpOddsBoardRow[]>>();

    filteredOddsRows.forEach((row) => {
      const market = marketLabelForRow(row);
      const book = row.bookie?.trim() || "Unknown Book";
      if (!marketMap.has(market)) marketMap.set(market, new Map());
      const bookMap = marketMap.get(market)!;
      if (!bookMap.has(book)) bookMap.set(book, []);
      bookMap.get(book)!.push(row);
    });

    return Array.from(marketMap.entries()).map(([market, bookMap]) => ({
      market,
      books: Array.from(bookMap.entries())
        .sort((a, b) => a[0].localeCompare(b[0]))
        .map(([book, rows]) => ({
          book,
          rows: [...rows].sort((a, b) => {
            const lineA = parseLineNumber(a.line_value);
            const lineB = parseLineNumber(b.line_value);
            if (lineA != null && lineB != null && lineA !== lineB) return lineA - lineB;
            const periodA = a.period_id ?? Number.MAX_SAFE_INTEGER;
            const periodB = b.period_id ?? Number.MAX_SAFE_INTEGER;
            if (periodA !== periodB) return periodA - periodB;
            const lineTextA = a.line_value ?? "";
            const lineTextB = b.line_value ?? "";
            if (lineTextA !== lineTextB) return lineTextA.localeCompare(lineTextB);
            return displayOutcomeLabel(a, homePlayer, awayPlayer).localeCompare(
              displayOutcomeLabel(b, homePlayer, awayPlayer)
            );
          }),
        })),
    })).map((marketGroup) => ({
      market: marketGroup.market,
      books: marketGroup.books.map((bookGroup) => {
        const overRows = bookGroup.rows.filter(isOverOutcome);
        const underRows = bookGroup.rows.filter(isUnderOutcome);
        const otherRows = bookGroup.rows.filter((row) => !isOverOutcome(row) && !isUnderOutcome(row));
        const splitOverUnder = hasTotalWord(marketGroup.market) || overRows.length > 0 || underRows.length > 0;
        return {
          book: bookGroup.book,
          rows: bookGroup.rows,
          overRows,
          underRows,
          otherRows,
          splitOverUnder,
        };
      }),
    }));
  }, [awayPlayer, filteredOddsRows, homePlayer]);

  useEffect(() => {
    if (selectedMarket === "all") return;
    const exists = marketOptions.some((option) => option.key === selectedMarket);
    if (!exists) setSelectedMarket("all");
  }, [marketOptions, selectedMarket]);

  const groupedRecentMatches = useMemo(() => {
    const rows = data?.recent_matches ?? [];
    const home: typeof rows = [];
    const away: typeof rows = [];
    const other: typeof rows = [];
    rows.forEach((row) => {
      if (row.side === "home") home.push(row);
      else if (row.side === "away") away.push(row);
      else other.push(row);
    });
    return { home, away, other };
  }, [data?.recent_matches]);

  const matchInfoRows = useMemo(() => {
    const info = (data?.match_info ?? {}) as Record<string, unknown>;
    return Object.entries(info).filter(([key]) => key !== "match_id");
  }, [data?.match_info]);

  function sectionChevron(section: SectionKey) {
    return expanded[section] ? "▾" : "▸";
  }

  function toggleSection(section: SectionKey) {
    setExpanded((prev) => ({ ...prev, [section]: !prev[section] }));
  }

  function betIdForSummary(side: "home" | "away", price: number): string {
    return `atp-${matchId}-moneyline-${side}-${price}-${startTimeUtc ?? ""}`;
  }

  function betIdForBoardRow(row: AtpOddsBoardRow, price: number): string {
    const market = marketLabelForRow(row);
    const outcome = displayOutcomeLabel(row, homePlayer, awayPlayer);
    const line = lineValueForOutcome(row) ?? "n/a";
    const book = row.bookie ?? "book";
    const period = row.period_name ?? (row.period_id != null ? String(row.period_id) : "all");
    return `atp-${matchId}-${market}-${period}-${book}-${outcome}-${line}-${price}`;
  }

  function isSavedSummary(side: "home" | "away"): boolean {
    const pick = side === "home" ? data?.odds_summary?.home : data?.odds_summary?.away;
    const price = pick?.odds_american ?? decimalToAmerican(pick?.odds_decimal);
    if (price == null) return false;
    return betslipItems.some((item) => item.id === betIdForSummary(side, price));
  }

  function saveSummary(side: "home" | "away") {
    const pick = side === "home" ? data?.odds_summary?.home : data?.odds_summary?.away;
    const price = pick?.odds_american ?? decimalToAmerican(pick?.odds_decimal);
    if (price == null) return;
    const player = side === "home" ? homePlayer : awayPlayer;
    const opponent = side === "home" ? awayPlayer : homePlayer;
    addToBetslip({
      id: betIdForSummary(side, price),
      player,
      playerId: null,
      opponent,
      tournamentName: tournamentName ?? undefined,
      round: roundName ?? undefined,
      matchTime: formatMatchTime(startTimeUtc),
      createdAt: new Date().toISOString(),
      market: "Moneyline",
      outcome: player,
      line: null,
      price,
      bookmaker: pick?.bookie ?? "Best Book",
      matchId: matchId || null,
      game: `${awayPlayer} vs ${homePlayer}`,
    });
    openDrawer();
  }

  function isSavedOddsRow(row: AtpOddsBoardRow): boolean {
    const price = toAmericanNumber(row);
    if (price == null) return false;
    return betslipItems.some((item) => item.id === betIdForBoardRow(row, price));
  }

  function saveOddsRow(row: AtpOddsBoardRow) {
    const price = toAmericanNumber(row);
    if (price == null) return;
    const outcome = displayOutcomeLabel(row, homePlayer, awayPlayer);
    const opponent =
      outcome === homePlayer ? awayPlayer : outcome === awayPlayer ? homePlayer : `${homePlayer} / ${awayPlayer}`;
    addToBetslip({
      id: betIdForBoardRow(row, price),
      player: outcome,
      playerId: null,
      opponent,
      tournamentName: tournamentName ?? undefined,
      round: roundName ?? undefined,
      matchTime: formatMatchTime(startTimeUtc),
      createdAt: new Date().toISOString(),
      market: marketLabelForRow(row),
      outcome,
      line: parseLineNumber(lineValueForOutcome(row)),
      price,
      bookmaker: row.bookie ?? "Unknown Book",
      matchId: matchId || null,
      game: `${awayPlayer} vs ${homePlayer}`,
    });
    openDrawer();
  }

  return (
    <ScrollView style={styles.screen} contentContainerStyle={styles.content}>
      <View style={styles.actionRow}>
        <Pressable onPress={() => router.back()} style={[styles.actionButton, { borderColor: colors.border.subtle }]}>
          <Text style={styles.actionText}>← Back</Text>
        </Pressable>
        <Pressable onPress={() => router.push("/(tabs)/home")} style={[styles.actionButton, { borderColor: colors.border.subtle }]}>
          <Text style={styles.actionText}>⌂ Home</Text>
        </Pressable>
      </View>

      <View style={[styles.panel, { borderColor: colors.border.subtle }]}>
        <Text style={styles.eyebrow}>ATP Matchup</Text>
        <AtpMatchupCard
          homePlayer={homePlayer}
          awayPlayer={awayPlayer}
          startTimeUtc={startTimeUtc}
          tournamentName={tournamentName}
          roundName={roundName}
          homeRank={homeRank}
          awayRank={awayRank}
          homeHeadshotUrl={homeHeadshotUrl}
          awayHeadshotUrl={awayHeadshotUrl}
          oddsSummary={data?.odds_summary}
          selectedOddsSide={isSavedSummary("home") ? "home" : isSavedSummary("away") ? "away" : null}
          onOddsPress={saveSummary}
        />
      </View>

      {loading ? <ActivityIndicator color="#A78BFA" /> : null}

      {error ? (
        <Pressable onPress={refetch} style={[styles.panel, { borderColor: colors.border.subtle }]}>
          <Text style={styles.errorTitle}>Failed to load ATP matchup detail.</Text>
          <Text style={styles.errorText}>{error}</Text>
          <Text style={styles.errorRetry}>Tap to retry</Text>
        </Pressable>
      ) : null}

      {data ? (
        <>
          <View style={[styles.panel, { borderColor: colors.border.subtle }]}>
            <Pressable onPress={() => toggleSection("oddsBoard")} style={styles.sectionHeader}>
              <Text style={styles.sectionTitle}>Odds Board</Text>
              <Text style={styles.sectionToggle}>{sectionChevron("oddsBoard")}</Text>
            </Pressable>
            {expanded.oddsBoard ? (
              filteredOddsRows.length ? (
                <>
                  <View style={styles.oddsMetaRow}>
                    <Text style={styles.oddsMetaText}>
                      Updated:{" "}
                      {data.odds_updated_at ? new Date(data.odds_updated_at).toLocaleString() : "Unknown"}
                    </Text>
                    <Pressable
                      style={[styles.copyButton, { borderColor: colors.border.subtle }]}
                      onPress={async () => {
                        await Clipboard.setStringAsync(buildOddsExport(filteredOddsRows, homePlayer, awayPlayer));
                        setCopyStatus("copied");
                        setTimeout(() => setCopyStatus("idle"), 1500);
                      }}
                    >
                      <Text style={styles.copyButtonText}>{copyStatus === "copied" ? "Copied" : "Copy TSV"}</Text>
                    </Pressable>
                  </View>

                  <View style={styles.toggleRow}>
                    <Pressable
                      style={[
                        styles.togglePill,
                        selectedMarket === "all" ? styles.togglePillActive : null,
                        { borderColor: colors.border.subtle },
                      ]}
                      onPress={() => setSelectedMarket("all")}
                    >
                      <Text style={[styles.toggleText, selectedMarket === "all" ? styles.toggleTextActive : null]}>
                        All
                      </Text>
                    </Pressable>
                    {marketOptions.map((option) => (
                      <Pressable
                        key={option.key}
                        style={[
                          styles.togglePill,
                          selectedMarket === option.key ? styles.togglePillActive : null,
                          { borderColor: colors.border.subtle },
                        ]}
                        onPress={() => setSelectedMarket(option.key)}
                      >
                        <Text
                          style={[
                            styles.toggleText,
                            selectedMarket === option.key ? styles.toggleTextActive : null,
                          ]}
                        >
                          {option.label}
                        </Text>
                      </Pressable>
                    ))}
                  </View>

                  {groupedOddsRows.map((marketGroup) => (
                    <View key={marketGroup.market} style={styles.marketGroup}>
                      <Text style={styles.marketTitle}>{marketGroup.market}</Text>
                      {marketGroup.books.map((bookGroup) => (
                        <View key={`${marketGroup.market}-${bookGroup.book}`} style={styles.bookGroup}>
                          <Text style={styles.bookTitle}>{bookGroup.book}</Text>
                          {bookGroup.splitOverUnder ? (
                            <>
                              {bookGroup.overRows.length ? (
                                <View style={styles.windowBlock}>
                                  <Text style={styles.windowTitle}>Overs</Text>
                                  <ScrollView horizontal showsHorizontalScrollIndicator={false} contentContainerStyle={styles.oddsWindowRow}>
                                    {bookGroup.overRows.map((row, idx) => {
                                      const disabled = toAmericanNumber(row) == null;
                                      const selected = isSavedOddsRow(row);
                                      return (
                                        <Pressable
                                          key={`${marketGroup.market}-${bookGroup.book}-over-${idx}`}
                                          disabled={disabled}
                                          onPress={() => saveOddsRow(row)}
                                          style={[
                                            styles.oddsCell,
                                            selected ? styles.oddsCellSelected : null,
                                            disabled ? styles.oddsCellDisabled : null,
                                          ]}
                                        >
                                          <Text style={styles.oddsCellOutcome}>{displayOutcomeLabel(row, homePlayer, awayPlayer)}</Text>
                                          {lineValueForOutcome(row) ? (
                                            <Text style={styles.oddsCellLine}>Line {lineValueForOutcome(row)}</Text>
                                          ) : null}
                                          <Text style={styles.oddsCellPrice}>{formatAmericanOnly(row)}</Text>
                                        </Pressable>
                                      );
                                    })}
                                  </ScrollView>
                                </View>
                              ) : null}

                              {bookGroup.underRows.length ? (
                                <View style={styles.windowBlock}>
                                  <Text style={styles.windowTitle}>Unders</Text>
                                  <ScrollView horizontal showsHorizontalScrollIndicator={false} contentContainerStyle={styles.oddsWindowRow}>
                                    {bookGroup.underRows.map((row, idx) => {
                                      const disabled = toAmericanNumber(row) == null;
                                      const selected = isSavedOddsRow(row);
                                      return (
                                        <Pressable
                                          key={`${marketGroup.market}-${bookGroup.book}-under-${idx}`}
                                          disabled={disabled}
                                          onPress={() => saveOddsRow(row)}
                                          style={[
                                            styles.oddsCell,
                                            selected ? styles.oddsCellSelected : null,
                                            disabled ? styles.oddsCellDisabled : null,
                                          ]}
                                        >
                                          <Text style={styles.oddsCellOutcome}>{displayOutcomeLabel(row, homePlayer, awayPlayer)}</Text>
                                          {lineValueForOutcome(row) ? (
                                            <Text style={styles.oddsCellLine}>Line {lineValueForOutcome(row)}</Text>
                                          ) : null}
                                          <Text style={styles.oddsCellPrice}>{formatAmericanOnly(row)}</Text>
                                        </Pressable>
                                      );
                                    })}
                                  </ScrollView>
                                </View>
                              ) : null}

                              {bookGroup.otherRows.length ? (
                                <View style={styles.windowBlock}>
                                  <Text style={styles.windowTitle}>Other</Text>
                                  <ScrollView horizontal showsHorizontalScrollIndicator={false} contentContainerStyle={styles.oddsWindowRow}>
                                    {bookGroup.otherRows.map((row, idx) => {
                                      const disabled = toAmericanNumber(row) == null;
                                      const selected = isSavedOddsRow(row);
                                      return (
                                        <Pressable
                                          key={`${marketGroup.market}-${bookGroup.book}-other-${idx}`}
                                          disabled={disabled}
                                          onPress={() => saveOddsRow(row)}
                                          style={[
                                            styles.oddsCell,
                                            selected ? styles.oddsCellSelected : null,
                                            disabled ? styles.oddsCellDisabled : null,
                                          ]}
                                        >
                                          <Text style={styles.oddsCellOutcome}>{displayOutcomeLabel(row, homePlayer, awayPlayer)}</Text>
                                          {lineValueForOutcome(row) ? (
                                            <Text style={styles.oddsCellLine}>Line {lineValueForOutcome(row)}</Text>
                                          ) : null}
                                          <Text style={styles.oddsCellPrice}>{formatAmericanOnly(row)}</Text>
                                        </Pressable>
                                      );
                                    })}
                                  </ScrollView>
                                </View>
                              ) : null}
                            </>
                          ) : (
                            <ScrollView horizontal showsHorizontalScrollIndicator={false} contentContainerStyle={styles.oddsWindowRow}>
                              {bookGroup.rows.map((row, idx) => {
                                const disabled = toAmericanNumber(row) == null;
                                const selected = isSavedOddsRow(row);
                                return (
                                  <Pressable
                                    key={`${marketGroup.market}-${bookGroup.book}-${idx}`}
                                    disabled={disabled}
                                    onPress={() => saveOddsRow(row)}
                                    style={[
                                      styles.oddsCell,
                                      selected ? styles.oddsCellSelected : null,
                                      disabled ? styles.oddsCellDisabled : null,
                                    ]}
                                  >
                                    <Text style={styles.oddsCellOutcome}>{displayOutcomeLabel(row, homePlayer, awayPlayer)}</Text>
                                    {lineValueForOutcome(row) ? (
                                      <Text style={styles.oddsCellLine}>Line {lineValueForOutcome(row)}</Text>
                                    ) : null}
                                    <Text style={styles.oddsCellPrice}>{formatAmericanOnly(row)}</Text>
                                  </Pressable>
                                );
                              })}
                            </ScrollView>
                          )}
                        </View>
                      ))}
                    </View>
                  ))}
                </>
              ) : (
                <Text style={styles.emptyText}>No odds found for this matchup yet.</Text>
              )
            ) : null}
          </View>

          <View style={[styles.panel, { borderColor: colors.border.subtle }]}>
            <Pressable onPress={() => toggleSection("h2hStats")} style={styles.sectionHeader}>
              <Text style={styles.sectionTitle}>Head-to-Head Stats</Text>
              <Text style={styles.sectionToggle}>{sectionChevron("h2hStats")}</Text>
            </Pressable>
            {expanded.h2hStats ? (
              data.head_to_head_stats.length ? (
                data.head_to_head_stats.map((row, idx) => (
                  <Text key={`h2h-${idx}`} style={styles.valueText}>
                    {formatShortDate(row.h2h_starttime)} • {row.h2h_ht ?? homePlayer} {row.h2h_hscore ?? "-"} -{" "}
                    {row.h2h_ascore ?? "-"} {row.h2h_at ?? awayPlayer}
                    {row.h2h_league_name ? ` • ${row.h2h_league_name}` : ""}
                  </Text>
                ))
              ) : (
                <Text style={styles.emptyText}>No head-to-head match rows available.</Text>
              )
            ) : null}
          </View>

          <View style={[styles.panel, { borderColor: colors.border.subtle }]}>
            <Pressable onPress={() => toggleSection("h2hSummary")} style={styles.sectionHeader}>
              <Text style={styles.sectionTitle}>Head-to-Head Summary</Text>
              <Text style={styles.sectionToggle}>{sectionChevron("h2hSummary")}</Text>
            </Pressable>
            {expanded.h2hSummary ? (
              data.head_to_head_summary ? (
                <>
                  <Text style={styles.valueText}>
                    {homePlayer}: {data.head_to_head_summary.ht_wins ?? 0} wins
                  </Text>
                  <Text style={styles.valueText}>
                    {awayPlayer}: {data.head_to_head_summary.at_wins ?? 0} wins
                  </Text>
                  <Text style={styles.valueText}>
                    Total played: {data.head_to_head_summary.played_matches ?? 0}
                  </Text>
                  {data.head_to_head_summary.period_years ? (
                    <Text style={styles.valueText}>Years: {data.head_to_head_summary.period_years}</Text>
                  ) : null}
                </>
              ) : (
                <Text style={styles.emptyText}>No head-to-head summary available.</Text>
              )
            ) : null}
          </View>

          <View style={[styles.panel, { borderColor: colors.border.subtle }]}>
            <Pressable onPress={() => toggleSection("playerHistory")} style={styles.sectionHeader}>
              <Text style={styles.sectionTitle}>Player Match History</Text>
              <Text style={styles.sectionToggle}>{sectionChevron("playerHistory")}</Text>
            </Pressable>
            {expanded.playerHistory ? (
              homeHistory || awayHistory ? (
                <View style={styles.historyGrid}>
                  {[
                    { label: homePlayer, history: homeHistory },
                    { label: awayPlayer, history: awayHistory },
                  ].map(({ label, history }) => (
                    <View key={label} style={styles.historyCard}>
                      <Text style={styles.historyCardTitle}>{history?.player_name ?? label}</Text>
                      <Text style={styles.valueText}>
                        L5: {history?.recent_form?.l5?.record ?? "—"}
                        {history?.recent_form?.l5?.sequence ? ` (${history.recent_form.l5.sequence})` : ""}
                      </Text>
                      <Text style={styles.valueText}>
                        L10: {history?.recent_form?.l10?.record ?? "—"}
                        {history?.recent_form?.l10?.sequence ? ` (${history.recent_form.l10.sequence})` : ""}
                      </Text>
                      <Text style={styles.valueText}>
                        Avg sets (L5 / L10): {formatDecimal(history?.averages?.l5?.avg_sets)} /{" "}
                        {formatDecimal(history?.averages?.l10?.avg_sets)}
                      </Text>
                      <Text style={styles.valueText}>
                        Avg total games (L5 / L10): {formatDecimal(history?.averages?.l5?.avg_total_games)} /{" "}
                        {formatDecimal(history?.averages?.l10?.avg_total_games)}
                      </Text>
                    </View>
                  ))}
                </View>
              ) : (
                <Text style={styles.emptyText}>No website match-history metrics are available for these players yet.</Text>
              )
            ) : null}
          </View>

          <View style={[styles.panel, { borderColor: colors.border.subtle }]}>
            <Pressable onPress={() => toggleSection("playerStatsAnalysis")} style={styles.sectionHeader}>
              <Text style={styles.sectionTitle}>Player Stats Analysis (L5/L10/L20)</Text>
              <Text style={styles.sectionToggle}>{sectionChevron("playerStatsAnalysis")}</Text>
            </Pressable>
            {expanded.playerStatsAnalysis ? (
              homeStatsAnalysis || awayStatsAnalysis ? (
                <View style={styles.historyGrid}>
                  {[
                    { label: homePlayer, analysis: homeStatsAnalysis },
                    { label: awayPlayer, analysis: awayStatsAnalysis },
                  ].map(({ label, analysis }) => (
                    <View key={`analysis-${label}`} style={styles.historyCard}>
                      <Text style={styles.historyCardTitle}>{analysis?.player_name ?? label}</Text>
                      {(["l5", "l10", "l20"] as const).map((windowKey) => {
                        const windowStats = analysis?.windows?.[windowKey];
                        return (
                          <View key={`${label}-${windowKey}`} style={styles.analysisWindowCard}>
                            <Text style={styles.groupTitle}>{windowKey.toUpperCase()}</Text>
                            <Text style={styles.valueText}>
                              Aces/match: {formatDecimal(windowStats?.aces_per_match?.value, 2)}
                            </Text>
                            <Text style={styles.valueText}>
                              DFs/match: {formatDecimal(windowStats?.double_faults_per_match?.value, 2)}
                            </Text>
                            <Text style={styles.valueText}>
                              First serve won: {formatPercent(windowStats?.first_serve_won_pct?.value)}
                            </Text>
                            <Text style={styles.valueText}>
                              Second serve won: {formatPercent(windowStats?.second_serve_won_pct?.value)}
                            </Text>
                            <Text style={styles.valueText}>
                              First serve return won: {formatPercent(windowStats?.first_serve_return_won_pct?.value)}
                            </Text>
                            <Text style={styles.valueText}>
                              Second serve return won: {formatPercent(windowStats?.second_serve_return_won_pct?.value)}
                            </Text>
                          </View>
                        );
                      })}
                    </View>
                  ))}
                </View>
              ) : (
                <Text style={styles.emptyText}>No Hawkeye player stats analysis is available for these players yet.</Text>
              )
            ) : null}
          </View>

          <View style={[styles.panel, { borderColor: colors.border.subtle }]}>
            <Pressable onPress={() => toggleSection("recentMatches")} style={styles.sectionHeader}>
              <Text style={styles.sectionTitle}>Recent Matches</Text>
              <Text style={styles.sectionToggle}>{sectionChevron("recentMatches")}</Text>
            </Pressable>
            {expanded.recentMatches ? (
              data.recent_matches.length ? (
                <>
                  <Text style={styles.groupTitle}>{homePlayer}</Text>
                  {groupedRecentMatches.home.length ? (
                    groupedRecentMatches.home.map((row, idx) => (
                      <Text key={`recent-home-${idx}`} style={styles.valueText}>
                        {formatShortDate(row.last_starttime)} • {row.last_ht ?? homePlayer} {row.last_hscore ?? "-"} -{" "}
                        {row.last_ascore ?? "-"} {row.last_at ?? awayPlayer} ({formatRecentOutcome(row.last_outcome)})
                      </Text>
                    ))
                  ) : (
                    <Text style={styles.emptyText}>No recent rows found.</Text>
                  )}

                  <Text style={styles.groupTitle}>{awayPlayer}</Text>
                  {groupedRecentMatches.away.length ? (
                    groupedRecentMatches.away.map((row, idx) => (
                      <Text key={`recent-away-${idx}`} style={styles.valueText}>
                        {formatShortDate(row.last_starttime)} • {row.last_ht ?? homePlayer} {row.last_hscore ?? "-"} -{" "}
                        {row.last_ascore ?? "-"} {row.last_at ?? awayPlayer} ({formatRecentOutcome(row.last_outcome)})
                      </Text>
                    ))
                  ) : (
                    <Text style={styles.emptyText}>No recent rows found.</Text>
                  )}

                  {groupedRecentMatches.other.length ? (
                    <>
                      <Text style={styles.groupTitle}>Other</Text>
                      {groupedRecentMatches.other.map((row, idx) => (
                        <Text key={`recent-other-${idx}`} style={styles.valueText}>
                          {formatShortDate(row.last_starttime)} • {row.last_ht ?? homePlayer} {row.last_hscore ?? "-"} -{" "}
                          {row.last_ascore ?? "-"} {row.last_at ?? awayPlayer} ({formatRecentOutcome(row.last_outcome)})
                        </Text>
                      ))}
                    </>
                  ) : null}
                </>
              ) : (
                <Text style={styles.emptyText}>No recent matches available.</Text>
              )
            ) : null}
          </View>

          <View style={[styles.panel, { borderColor: colors.border.subtle }]}>
            <Pressable onPress={() => toggleSection("bettingInfo")} style={styles.sectionHeader}>
              <Text style={styles.sectionTitle}>Betting Info</Text>
              <Text style={styles.sectionToggle}>{sectionChevron("bettingInfo")}</Text>
            </Pressable>
            {expanded.bettingInfo ? (
              data.betting_info.length ? (
                data.betting_info.map((row, idx) => {
                  const categoryLabel = titleCaseWords(row.category);
                  const subTabLabel = titleCaseWords(row.sub_tab);
                  const statLabel = titleCaseWords(row.label) || "Stat";
                  const lineLabel = row.value != null && `${row.value}`.trim() ? `${row.value}` : null;
                  return (
                    <View key={`bet-${idx}`} style={styles.bettingStatCard}>
                      <Text style={styles.bettingStatTitle}>{statLabel}</Text>
                      {(categoryLabel || subTabLabel) && (
                        <Text style={styles.bettingStatMeta}>
                          {[categoryLabel, subTabLabel].filter(Boolean).join(" • ")}
                        </Text>
                      )}
                      {lineLabel ? <Text style={styles.bettingStatLine}>Line {lineLabel}</Text> : null}
                      <View style={styles.bettingStatValuesRow}>
                        <View style={styles.bettingStatValueCol}>
                          <Text style={styles.bettingStatTeam}>{homePlayer}</Text>
                          <Text style={styles.bettingStatValue}>{formatBettingMetric(row.home, row.label)}</Text>
                          {row.total_matches_home != null ? (
                            <Text style={styles.bettingStatSample}>n={row.total_matches_home}</Text>
                          ) : null}
                        </View>
                        <View style={styles.bettingStatValueCol}>
                          <Text style={[styles.bettingStatTeam, styles.bettingStatTeamRight]}>{awayPlayer}</Text>
                          <Text style={[styles.bettingStatValue, styles.bettingStatValueRight]}>
                            {formatBettingMetric(row.away, row.label)}
                          </Text>
                          {row.total_matches_away != null ? (
                            <Text style={[styles.bettingStatSample, styles.bettingStatValueRight]}>
                              n={row.total_matches_away}
                            </Text>
                          ) : null}
                        </View>
                      </View>
                    </View>
                  );
                })
              ) : (
                <Text style={styles.emptyText}>No betting info rows available.</Text>
              )
            ) : null}
          </View>

          <View style={[styles.panel, { borderColor: colors.border.subtle }]}>
            <Pressable onPress={() => toggleSection("matchInfo")} style={styles.sectionHeader}>
              <Text style={styles.sectionTitle}>Match Info</Text>
              <Text style={styles.sectionToggle}>{sectionChevron("matchInfo")}</Text>
            </Pressable>
            {expanded.matchInfo ? (
              matchInfoRows.length ? (
                matchInfoRows.map(([key, value]) => (
                  <View key={key} style={styles.row}>
                    <Text style={styles.keyText}>{key}</Text>
                    <Text style={styles.valueText}>{stringifyValue(value)}</Text>
                  </View>
                ))
              ) : (
                <Text style={styles.emptyText}>No match info available.</Text>
              )
            ) : null}
          </View>
        </>
      ) : null}
    </ScrollView>
  );
}

const styles = StyleSheet.create({
  screen: { flex: 1, backgroundColor: "#050A18" },
  content: { padding: 16, gap: 10, paddingBottom: 40 },
  actionRow: { flexDirection: "row", gap: 8 },
  actionButton: {
    borderWidth: StyleSheet.hairlineWidth,
    borderRadius: 12,
    backgroundColor: "#0B1529",
    paddingHorizontal: 12,
    paddingVertical: 8,
  },
  actionText: { color: "#E9F2FF", fontWeight: "700", fontSize: 12 },
  panel: {
    borderWidth: StyleSheet.hairlineWidth,
    borderRadius: 14,
    backgroundColor: "#0B1529",
    padding: 12,
    gap: 8,
  },
  eyebrow: { color: "#90B3E9", fontSize: 11, fontWeight: "700" },
  sectionTitle: { color: "#93C5FD", fontSize: 14, fontWeight: "700" },
  sectionHeader: { flexDirection: "row", alignItems: "center", justifyContent: "space-between" },
  sectionToggle: { color: "#C4B5FD", fontWeight: "800", fontSize: 14 },
  groupTitle: { color: "#A7C0E8", fontSize: 12, fontWeight: "800", marginTop: 6 },
  oddsMetaRow: { flexDirection: "row", alignItems: "center", justifyContent: "space-between", gap: 8 },
  oddsMetaText: { color: "#94A3B8", fontSize: 11 },
  copyButton: {
    borderWidth: StyleSheet.hairlineWidth,
    borderRadius: 8,
    backgroundColor: "#0F172A",
    paddingHorizontal: 10,
    paddingVertical: 6,
  },
  copyButtonText: { color: "#BFDBFE", fontSize: 11, fontWeight: "700" },
  toggleRow: { flexDirection: "row", flexWrap: "wrap", gap: 6 },
  togglePill: {
    borderWidth: StyleSheet.hairlineWidth,
    borderRadius: 999,
    backgroundColor: "#0F172A",
    paddingHorizontal: 10,
    paddingVertical: 5,
  },
  togglePillActive: {
    backgroundColor: "#1D4ED8",
    borderColor: "#1D4ED8",
  },
  toggleText: { color: "#BFDBFE", fontSize: 11, fontWeight: "700" },
  toggleTextActive: { color: "#EFF6FF" },
  marketGroup: {
    borderTopWidth: StyleSheet.hairlineWidth,
    borderTopColor: "#1E293B",
    paddingTop: 8,
    marginTop: 2,
    gap: 6,
  },
  marketTitle: { color: "#E2E8F0", fontSize: 13, fontWeight: "800" },
  bookGroup: { gap: 4 },
  bookTitle: { color: "#93C5FD", fontSize: 12, fontWeight: "700" },
  windowBlock: { gap: 4 },
  windowTitle: { color: "#A7C0E8", fontSize: 11, fontWeight: "700" },
  oddsWindowRow: { flexDirection: "row", gap: 8, paddingRight: 4 },
  oddsCell: {
    width: 118,
    borderWidth: StyleSheet.hairlineWidth,
    borderColor: "#334155",
    borderRadius: 10,
    backgroundColor: "#0F172A",
    paddingVertical: 8,
    paddingHorizontal: 8,
    gap: 2,
  },
  oddsCellSelected: {
    borderColor: "#3B82F6",
    backgroundColor: "rgba(59,130,246,0.2)",
  },
  oddsCellDisabled: {
    opacity: 0.5,
  },
  oddsCellOutcome: { color: "#E2E8F0", fontSize: 12, fontWeight: "700" },
  oddsCellLine: { color: "#94A3B8", fontSize: 10, fontWeight: "600" },
  oddsCellPrice: { color: "#86EFAC", fontSize: 15, fontWeight: "800" },
  historyGrid: { gap: 8 },
  historyCard: {
    borderWidth: StyleSheet.hairlineWidth,
    borderColor: "#1E293B",
    borderRadius: 10,
    backgroundColor: "#0F172A",
    padding: 10,
    gap: 4,
  },
  historyCardTitle: { color: "#E2E8F0", fontSize: 12, fontWeight: "800" },
  analysisWindowCard: {
    borderWidth: StyleSheet.hairlineWidth,
    borderColor: "#1E293B",
    borderRadius: 10,
    backgroundColor: "#0F172A",
    padding: 8,
    gap: 2,
    marginTop: 6,
  },
  bettingStatCard: {
    borderWidth: StyleSheet.hairlineWidth,
    borderColor: "#1E293B",
    borderRadius: 10,
    backgroundColor: "#0F172A",
    padding: 10,
    gap: 4,
  },
  bettingStatTitle: { color: "#E2E8F0", fontSize: 12, fontWeight: "800" },
  bettingStatMeta: { color: "#93C5FD", fontSize: 10, fontWeight: "700" },
  bettingStatLine: { color: "#94A3B8", fontSize: 11, fontWeight: "600" },
  bettingStatValuesRow: { flexDirection: "row", justifyContent: "space-between", gap: 12, marginTop: 4 },
  bettingStatValueCol: { flex: 1, gap: 2 },
  bettingStatTeam: { color: "#A7C0E8", fontSize: 10, fontWeight: "700" },
  bettingStatTeamRight: { textAlign: "right" },
  bettingStatValue: { color: "#E5E7EB", fontSize: 13, fontWeight: "800" },
  bettingStatValueRight: { textAlign: "right" },
  bettingStatSample: { color: "#94A3B8", fontSize: 10, fontWeight: "600" },
  row: { gap: 2 },
  keyText: { color: "#C4B5FD", fontSize: 11, fontWeight: "700" },
  valueText: { color: "#E5E7EB", fontSize: 12, lineHeight: 18 },
  emptyText: { color: "#94A3B8", fontSize: 12 },
  errorTitle: { color: "#FCA5A5", fontWeight: "700" },
  errorText: { color: "#FECACA", fontSize: 12 },
  errorRetry: { color: "#E5E7EB", marginTop: 4, fontSize: 12 },
});
