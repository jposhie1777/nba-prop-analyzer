import { useEffect, useMemo, useState } from "react";
import { ActivityIndicator, Pressable, ScrollView, StyleSheet, Text, View } from "react-native";
import { useLocalSearchParams, useRouter } from "expo-router";
import * as Clipboard from "expo-clipboard";

import {
  SoccerLeague,
  SoccerOddsBoardRow,
  useSoccerMatchupDetail,
} from "@/hooks/soccer/useSoccerMatchups";
import { useTheme } from "@/store/useTheme";
import { useSoccerLeagueBadges } from "@/hooks/soccer/useSoccerLeagueBadges";
import { resolveBadgeForTeam } from "@/utils/soccerDisplay";
import { MatchupSlugCard } from "@/components/soccer/MatchupSlugCard";
import { useSoccerBetslip } from "@/store/useSoccerBetslip";
import { useSoccerBetslipDrawer } from "@/store/useSoccerBetslipDrawer";

type Props = {
  league: SoccerLeague;
  leagueTitle: string;
};

type SectionKey = "odds" | "matchInfo" | "matchKeys" | "bettingStats" | "lastMatches";

type LastMatchRow = {
  side?: string | null;
  lm_date?: string | null;
  lm_ht?: string | null;
  lm_at?: string | null;
  lm_hscore?: number | null;
  lm_ascore?: number | null;
  lm_outcome?: string | null;
};

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

function toTimestamp(value?: string | null) {
  if (!value) return 0;
  const time = new Date(value).getTime();
  return Number.isNaN(time) ? 0 : time;
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

function formatAmericanOnly(
  row: Pick<SoccerOddsBoardRow, "odds_american" | "odds_decimal">
): string {
  if (row.odds_american != null) return formatOddsAmerican(row.odds_american);
  const converted = decimalToAmerican(row.odds_decimal);
  if (converted == null) return "—";
  return formatOddsAmerican(converted);
}

function toAmericanNumber(
  row: Pick<SoccerOddsBoardRow, "odds_american" | "odds_decimal">
): number | null {
  if (row.odds_american != null) return row.odds_american;
  return decimalToAmerican(row.odds_decimal);
}

function marketLabelForRow(row: SoccerOddsBoardRow): string {
  const raw = row.market_group ?? row.market;
  const cleaned = raw?.trim();
  return cleaned || "Other";
}

function marketKey(label: string): string {
  return label.trim().toLowerCase();
}

function periodLabel(row: SoccerOddsBoardRow): string {
  if (row.period_name) return row.period_name;
  if (row.period_id != null) return `Period ${row.period_id}`;
  return "All";
}

function outcomeLabel(row: SoccerOddsBoardRow): string {
  const raw = row.outcome_name?.trim();
  if (!raw) return "Outcome";
  if (/^o1$/i.test(raw)) return "Home";
  if (/^o2$/i.test(raw)) return "Draw";
  if (/^o3$/i.test(raw)) return "Away";
  return raw;
}

function isOverOutcome(row: SoccerOddsBoardRow): boolean {
  const label = outcomeLabel(row).toLowerCase();
  return label.startsWith("over");
}

function isUnderOutcome(row: SoccerOddsBoardRow): boolean {
  const label = outcomeLabel(row).toLowerCase();
  return label.startsWith("under");
}

function hasTotalWord(value: string): boolean {
  return value.toLowerCase().includes("total");
}

function buildOddsExport(rows: SoccerOddsBoardRow[]): string {
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
      outcomeLabel(row),
      row.bookie ?? "",
      formatAmericanOnly(row),
    ].join("\t")
  );
  return [header, ...lines].join("\n");
}

function LastMatchLine({ row }: { row: LastMatchRow }) {
  return (
    <Text style={styles.valueText}>
      {formatShortDate(row.lm_date)} - {row.lm_ht ?? "Home"} {row.lm_hscore ?? "-"} - {row.lm_ascore ?? "-"}{" "}
      {row.lm_at ?? "Away"} ({row.lm_outcome ?? "N/A"})
    </Text>
  );
}

export function LeagueMatchupDetailScreen({ league, leagueTitle }: Props) {
  const router = useRouter();
  const { colors } = useTheme();
  const betslipItems = useSoccerBetslip((s) => s.items);
  const addToBetslip = useSoccerBetslip((s) => s.add);
  const openDrawer = useSoccerBetslipDrawer((s) => s.open);
  const params = useLocalSearchParams<{
    matchId?: string;
    homeTeam?: string;
    awayTeam?: string;
    startTimeUtc?: string;
    homeRecord?: string;
    awayRecord?: string;
    homeLogoUri?: string;
    awayLogoUri?: string;
  }>();
  const matchId = Number(params.matchId);
  const { data, loading, error, refetch } = useSoccerMatchupDetail(league, Number.isFinite(matchId) ? matchId : null);
  const { data: badgeMap } = useSoccerLeagueBadges(league);

  const matchInfo = (data?.match_info ?? {}) as Record<string, unknown>;
  const homeTeam = params.homeTeam ?? (matchInfo.home_team as string | undefined) ?? "Home";
  const awayTeam = params.awayTeam ?? (matchInfo.away_team as string | undefined) ?? "Away";
  const startTimeUtc = params.startTimeUtc ?? (matchInfo.date_utc as string | undefined) ?? null;
  const homeRecentForm = (matchInfo.home_form as string | undefined) ?? "-";
  const awayRecentForm = (matchInfo.away_form as string | undefined) ?? "-";
  const homeRecord = params.homeRecord ?? homeRecentForm;
  const awayRecord = params.awayRecord ?? awayRecentForm;
  const homeLogoUri =
    params.homeLogoUri ??
    resolveBadgeForTeam(league, homeTeam, badgeMap);
  const awayLogoUri =
    params.awayLogoUri ??
    resolveBadgeForTeam(league, awayTeam, badgeMap);
  const [expanded, setExpanded] = useState<Record<SectionKey, boolean>>({
    odds: true,
    matchInfo: false,
    matchKeys: false,
    bettingStats: false,
    lastMatches: false,
  });
  const [copyStatus, setCopyStatus] = useState<"idle" | "copied">("idle");
  const [selectedMarket, setSelectedMarket] = useState<string>("all");

  const matchInfoRows = Object.entries(matchInfo).filter(([key]) => !["match_id", "home_team", "away_team"].includes(key));
  const groupedLastMatches = useMemo(() => {
    const rows = (data?.last_matches ?? []) as LastMatchRow[];
    const home: LastMatchRow[] = [];
    const away: LastMatchRow[] = [];
    const other: LastMatchRow[] = [];

    rows.forEach((row) => {
      if (row.side === "home") {
        home.push(row);
      } else if (row.side === "away") {
        away.push(row);
      } else {
        other.push(row);
      }
    });

    const sortByDateDesc = (a: LastMatchRow, b: LastMatchRow) => toTimestamp(b.lm_date) - toTimestamp(a.lm_date);
    home.sort(sortByDateDesc);
    away.sort(sortByDateDesc);
    other.sort(sortByDateDesc);

    return { home, away, other };
  }, [data?.last_matches]);
  const marketOptions = useMemo(() => {
    const seen = new Map<string, string>();
    (data?.odds_board ?? []).forEach((row) => {
      const label = marketLabelForRow(row);
      const key = marketKey(label);
      if (!seen.has(key)) {
        seen.set(key, label);
      }
    });
    return Array.from(seen.entries()).map(([key, label]) => ({ key, label }));
  }, [data?.odds_board]);
  const filteredOddsRows = useMemo(() => {
    const rows = data?.odds_board ?? [];
    if (selectedMarket === "all") return rows;
    return rows.filter((row) => marketKey(marketLabelForRow(row)) === selectedMarket);
  }, [data?.odds_board, selectedMarket]);
  const groupedOddsRows = useMemo(() => {
    const marketMap = new Map<string, Map<string, SoccerOddsBoardRow[]>>();

    filteredOddsRows.forEach((row) => {
      const market = marketLabelForRow(row);
      const book = row.bookie?.trim() || "Unknown Book";
      if (!marketMap.has(market)) {
        marketMap.set(market, new Map());
      }
      const bookMap = marketMap.get(market)!;
      if (!bookMap.has(book)) {
        bookMap.set(book, []);
      }
      bookMap.get(book)!.push(row);
    });

    return Array.from(marketMap.entries()).map(([market, bookMap]) => ({
      market,
      books: Array.from(bookMap.entries())
        .sort((a, b) => a[0].localeCompare(b[0]))
        .map(([book, rows]) => ({
          book,
          rows: [...rows].sort((a, b) => {
            const periodA = a.period_id ?? Number.MAX_SAFE_INTEGER;
            const periodB = b.period_id ?? Number.MAX_SAFE_INTEGER;
            if (periodA !== periodB) return periodA - periodB;
            const lineA = a.line_value ?? "";
            const lineB = b.line_value ?? "";
            if (lineA !== lineB) return lineA.localeCompare(lineB);
            return outcomeLabel(a).localeCompare(outcomeLabel(b));
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
  }, [filteredOddsRows]);

  useEffect(() => {
    if (selectedMarket === "all") return;
    const stillExists = marketOptions.some((option) => option.key === selectedMarket);
    if (!stillExists) {
      setSelectedMarket("all");
    }
  }, [marketOptions, selectedMarket]);

  function lineNumber(value?: string | null): number | null {
    if (!value) return null;
    const parsed = Number(value);
    return Number.isFinite(parsed) ? parsed : null;
  }

  function betIdForRow(row: SoccerOddsBoardRow, price: number): string {
    const market = marketLabelForRow(row);
    const period = periodLabel(row);
    const outcome = outcomeLabel(row);
    const line = row.line_value ?? "n/a";
    const book = row.bookie ?? "book";
    return `${league}-${matchId}-${market}-${period}-${book}-${outcome}-${line}-${price}`;
  }

  function saveOddsRow(row: SoccerOddsBoardRow) {
    const price = toAmericanNumber(row);
    if (price == null) return;
    addToBetslip({
      id: betIdForRow(row, price),
      league: league.toUpperCase(),
      game: `${awayTeam} @ ${homeTeam}`,
      start_time_et: startTimeUtc ?? undefined,
      market: marketLabelForRow(row),
      outcome: outcomeLabel(row),
      line: lineNumber(row.line_value),
      price,
      bookmaker: row.bookie ?? "Unknown Book",
      rationale: `Saved from ${marketLabelForRow(row)} odds board`,
    });
    openDrawer();
  }

  function isSavedOddsRow(row: SoccerOddsBoardRow): boolean {
    const price = toAmericanNumber(row);
    if (price == null) return false;
    const id = betIdForRow(row, price);
    return betslipItems.some((item) => item.id === id);
  }

  function toggleSection(section: SectionKey) {
    setExpanded((prev) => ({ ...prev, [section]: !prev[section] }));
  }

  function sectionChevron(section: SectionKey) {
    return expanded[section] ? "▾" : "▸";
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
        <Text style={styles.eyebrow}>{leagueTitle}</Text>
        <MatchupSlugCard
          league={league}
          homeTeam={homeTeam}
          awayTeam={awayTeam}
          startTimeUtc={startTimeUtc}
          homeRecord={homeRecord}
          awayRecord={awayRecord}
          homeLogoUri={homeLogoUri}
          awayLogoUri={awayLogoUri}
          oddsSummary={data?.odds_summary}
        />
      </View>

      {loading ? <ActivityIndicator color="#A78BFA" /> : null}

      {error ? (
        <Pressable onPress={refetch} style={[styles.panel, { borderColor: colors.border.subtle }]}>
          <Text style={styles.errorTitle}>Failed to load matchup detail.</Text>
          <Text style={styles.errorText}>{error}</Text>
          <Text style={styles.errorRetry}>Tap to retry</Text>
        </Pressable>
      ) : null}

      {data ? (
        <>
          <View style={[styles.panel, { borderColor: colors.border.subtle }]}>
            <Pressable onPress={() => toggleSection("odds")} style={styles.sectionHeader}>
              <Text style={styles.sectionTitle}>Odds Board</Text>
              <Text style={styles.sectionToggle}>{sectionChevron("odds")}</Text>
            </Pressable>
            {expanded.odds ? (
              filteredOddsRows.length ? (
                <>
                  <View style={styles.oddsMetaRow}>
                    <Text style={styles.oddsMetaText}>
                      Updated:{" "}
                      {data.odds_updated_at
                        ? new Date(data.odds_updated_at).toLocaleString()
                        : "Unknown"}
                    </Text>
                    <Pressable
                      style={[styles.copyButton, { borderColor: colors.border.subtle }]}
                      onPress={async () => {
                        await Clipboard.setStringAsync(buildOddsExport(filteredOddsRows));
                        setCopyStatus("copied");
                        setTimeout(() => setCopyStatus("idle"), 1500);
                      }}
                    >
                      <Text style={styles.copyButtonText}>
                        {copyStatus === "copied" ? "Copied" : "Copy TSV"}
                      </Text>
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
                                  <ScrollView
                                    horizontal
                                    showsHorizontalScrollIndicator={false}
                                    contentContainerStyle={styles.oddsWindowRow}
                                  >
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
                                          <Text style={styles.oddsCellOutcome}>{outcomeLabel(row)}</Text>
                                          <Text style={styles.oddsCellLine}>
                                            {row.line_value ? `Line ${row.line_value}` : periodLabel(row)}
                                          </Text>
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
                                  <ScrollView
                                    horizontal
                                    showsHorizontalScrollIndicator={false}
                                    contentContainerStyle={styles.oddsWindowRow}
                                  >
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
                                          <Text style={styles.oddsCellOutcome}>{outcomeLabel(row)}</Text>
                                          <Text style={styles.oddsCellLine}>
                                            {row.line_value ? `Line ${row.line_value}` : periodLabel(row)}
                                          </Text>
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
                                  <ScrollView
                                    horizontal
                                    showsHorizontalScrollIndicator={false}
                                    contentContainerStyle={styles.oddsWindowRow}
                                  >
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
                                          <Text style={styles.oddsCellOutcome}>{outcomeLabel(row)}</Text>
                                          <Text style={styles.oddsCellLine}>
                                            {row.line_value ? `Line ${row.line_value}` : periodLabel(row)}
                                          </Text>
                                          <Text style={styles.oddsCellPrice}>{formatAmericanOnly(row)}</Text>
                                        </Pressable>
                                      );
                                    })}
                                  </ScrollView>
                                </View>
                              ) : null}
                            </>
                          ) : (
                            <ScrollView
                              horizontal
                              showsHorizontalScrollIndicator={false}
                              contentContainerStyle={styles.oddsWindowRow}
                            >
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
                                    <Text style={styles.oddsCellOutcome}>{outcomeLabel(row)}</Text>
                                    <Text style={styles.oddsCellLine}>
                                      {row.line_value ? `Line ${row.line_value}` : periodLabel(row)}
                                    </Text>
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

          <View style={[styles.panel, { borderColor: colors.border.subtle }]}>
            <Pressable onPress={() => toggleSection("matchKeys")} style={styles.sectionHeader}>
              <Text style={styles.sectionTitle}>Match Keys - Betting Insights</Text>
              <Text style={styles.sectionToggle}>{sectionChevron("matchKeys")}</Text>
            </Pressable>
            {expanded.matchKeys ? (
              data.match_keys.length ? (
                data.match_keys.map((row, idx) => (
                  <Text key={`mk-${idx}`} style={styles.valueText}>
                    #{row.rank ?? idx + 1} {row.statement ?? "No statement"}
                  </Text>
                ))
              ) : (
                <Text style={styles.emptyText}>No match keys available.</Text>
              )
            ) : null}
          </View>

          <View style={[styles.panel, { borderColor: colors.border.subtle }]}>
            <Pressable onPress={() => toggleSection("bettingStats")} style={styles.sectionHeader}>
              <Text style={styles.sectionTitle}>Betting Stats</Text>
              <Text style={styles.sectionToggle}>{sectionChevron("bettingStats")}</Text>
            </Pressable>
            {expanded.bettingStats ? (
              data.betting_stats.length ? (
                data.betting_stats.map((row, idx) => (
                  <Text key={`bs-${idx}`} style={styles.valueText}>
                    {row.category ?? "category"} • {row.sub_tab ?? "sub_tab"} • {row.label ?? "label"}: {row.value ?? "–"}
                  </Text>
                ))
              ) : (
                <Text style={styles.emptyText}>No betting stats available.</Text>
              )
            ) : null}
          </View>

          <View style={[styles.panel, { borderColor: colors.border.subtle }]}>
            <Pressable onPress={() => toggleSection("lastMatches")} style={styles.sectionHeader}>
              <Text style={styles.sectionTitle}>Last Matches</Text>
              <Text style={styles.sectionToggle}>{sectionChevron("lastMatches")}</Text>
            </Pressable>
            {expanded.lastMatches ? (
              data.last_matches.length ? (
                <>
                  <Text style={styles.groupTitle}>{homeTeam}</Text>
                  {groupedLastMatches.home.length ? (
                    groupedLastMatches.home.map((row, idx) => <LastMatchLine key={`home-lm-${idx}`} row={row} />)
                  ) : (
                    <Text style={styles.emptyText}>No recent matches found.</Text>
                  )}

                  <Text style={styles.groupTitle}>{awayTeam}</Text>
                  {groupedLastMatches.away.length ? (
                    groupedLastMatches.away.map((row, idx) => <LastMatchLine key={`away-lm-${idx}`} row={row} />)
                  ) : (
                    <Text style={styles.emptyText}>No recent matches found.</Text>
                  )}

                  {groupedLastMatches.other.length ? (
                    <>
                      <Text style={styles.groupTitle}>Other</Text>
                      {groupedLastMatches.other.map((row, idx) => <LastMatchLine key={`other-lm-${idx}`} row={row} />)}
                    </>
                  ) : null}
                </>
              ) : (
                <Text style={styles.emptyText}>No last matches available.</Text>
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
  panel: { borderWidth: StyleSheet.hairlineWidth, borderRadius: 14, backgroundColor: "#0B1529", padding: 12, gap: 8 },
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
    width: 106,
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
