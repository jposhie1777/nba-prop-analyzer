import { useEffect, useMemo, useState } from "react";
import { ActivityIndicator, Pressable, ScrollView, StyleSheet, Text, View } from "react-native";
import { useLocalSearchParams, useRouter } from "expo-router";
import * as Clipboard from "expo-clipboard";

import { AtpMatchupCard } from "@/components/atp/AtpMatchupCard";
import { AtpOddsBoardRow, useAtpMatchupDetail } from "@/hooks/atp/useAtpMatchups";
import { useAtpBetslip } from "@/store/useAtpBetslip";
import { useAtpBetslipDrawer } from "@/store/useAtpBetslipDrawer";
import { useTheme } from "@/store/useTheme";

type SectionKey =
  | "oddsBoard"
  | "h2hStats"
  | "h2hSummary"
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
      row.line_value ?? "",
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
    const line = row.line_value ?? "n/a";
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
      line: parseLineNumber(row.line_value),
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
                                          {row.line_value ? <Text style={styles.oddsCellLine}>Line {row.line_value}</Text> : null}
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
                                          {row.line_value ? <Text style={styles.oddsCellLine}>Line {row.line_value}</Text> : null}
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
                                          {row.line_value ? <Text style={styles.oddsCellLine}>Line {row.line_value}</Text> : null}
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
                                    {row.line_value ? <Text style={styles.oddsCellLine}>Line {row.line_value}</Text> : null}
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
                  const valuePart = row.value != null && `${row.value}`.trim() ? `${row.value}` : null;
                  const homePart = row.home != null ? `${homePlayer}: ${row.home}` : null;
                  const awayPart = row.away != null ? `${awayPlayer}: ${row.away}` : null;
                  const sampleParts = [
                    row.total_matches_home != null ? `${homePlayer} n=${row.total_matches_home}` : null,
                    row.total_matches_away != null ? `${awayPlayer} n=${row.total_matches_away}` : null,
                  ].filter(Boolean);
                  const detail = [valuePart, homePart, awayPart, ...sampleParts].filter(Boolean).join(" • ");
                  return (
                    <Text key={`bet-${idx}`} style={styles.valueText}>
                      {row.category ?? "category"} • {row.sub_tab ?? "sub_tab"} • {row.label ?? "label"}: {detail || "–"}
                    </Text>
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
  row: { gap: 2 },
  keyText: { color: "#C4B5FD", fontSize: 11, fontWeight: "700" },
  valueText: { color: "#E5E7EB", fontSize: 12, lineHeight: 18 },
  emptyText: { color: "#94A3B8", fontSize: 12 },
  errorTitle: { color: "#FCA5A5", fontWeight: "700" },
  errorText: { color: "#FECACA", fontSize: 12 },
  errorRetry: { color: "#E5E7EB", marginTop: 4, fontSize: 12 },
});
