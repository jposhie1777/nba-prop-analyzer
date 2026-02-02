// /app/(tabs)/live-props-dev.tsx
import {
  View,
  Text,
  FlatList,
  StyleSheet,
  Pressable,
  Image,
} from "react-native";
import { useCallback, useMemo, useState } from "react";

import { useTheme } from "@/store/useTheme";
import LivePropCard from "@/components/live/LivePropCard";
import { LivePropFilterBar } from "@/components/live/LivePropFilterBar";
import { TEAM_LOGOS } from "@/utils/teamLogos";

// 🔁 Cursor-based pagination hook
import { useLivePropsInfinite } from "@/hooks/useLivePropsInfinite";

/* ======================================================
   TYPES
====================================================== */

type MarketType = "OVER" | "UNDER" | "MILESTONE";

type Filters = {
  stats: string[];           // [] = ALL
  marketTypes: MarketType[]; // [] = ALL
  minOdds: number;
  maxOdds: number;
};

type GameGroup = {
  gameKey: string;
  gameId?: string | number;
  home_team_abbr?: string;
  away_team_abbr?: string;
  game_period?: string | null;
  game_clock?: string | null;
  game_state?: string | null;
  startTimeMs: number | null;
  props: any[];
};

/* ======================================================
   CONFIG
====================================================== */

// Safety cap so mobile never explodes
const MAX_ROWS = 600;

const DEFAULT_FILTERS: Filters = {
  stats: [],
  marketTypes: [],
  minOdds: -800,
  maxOdds: 400,
};

/* ======================================================
   FILTERING
====================================================== */

function applyFilters(data: any[], filters: Filters) {
  return data.filter((item) => {
    // ---- STAT FILTER ----
    if (
      filters.stats.length > 0 &&
      !filters.stats.includes(item.market)
    ) {
      return false;
    }

    // ---- MARKET TYPE FILTER ----
    if (
      filters.marketTypes.length > 0 &&
      !filters.marketTypes.includes(item.display_odds_side)
    ) {
      return false;
    }

    // ---- ODDS FILTER ----
    if (item.display_odds == null) return false;

    if (
      item.display_odds < filters.minOdds ||
      item.display_odds > filters.maxOdds
    ) {
      return false;
    }

    return true;
  });
}

function parseStartTime(value: any): number | null {
  if (!value) return null;
  if (value instanceof Date) {
    return Number.isNaN(value.getTime()) ? null : value.getTime();
  }
  if (typeof value === "number") return value;
  if (typeof value === "string") {
    const parsed = Date.parse(value);
    return Number.isNaN(parsed) ? null : parsed;
  }
  return null;
}

function resolveGameStartTime(item: any): number | null {
  const candidates = [
    item.game_start_time,
    item.game_start_time_et,
    item.game_start_time_est,
    item.start_time_et,
    item.start_time_est,
    item.start_time_utc,
    item.game_start_time_utc,
    item.start_time,
    item.game_time,
  ];

  for (const candidate of candidates) {
    const parsed = parseStartTime(candidate);
    if (parsed != null) return parsed;
  }

  return null;
}

function resolveGameKey(item: any): string {
  if (item.game_id != null) return String(item.game_id);
  return `${item.away_team_abbr ?? "AWAY"}-${item.home_team_abbr ?? "HOME"}`;
}

function formatGameStatus(
  period?: string | null,
  clock?: string | null,
  state?: string | null
) {
  const normalizedState =
    typeof state === "string" ? state.toLowerCase() : null;

  if (normalizedState === "halftime") return "Halftime";
  if (normalizedState === "final") return "Final";
  if (period && clock) return `${period} ${clock}`;
  if (period) return period;
  if (clock) return clock;
  return "Live";
}

/* ======================================================
   SCREEN
====================================================== */

export default function LivePropsDevScreen() {
  const { colors } = useTheme();
  const [filters, setFilters] = useState<Filters>(DEFAULT_FILTERS);
  const [expandedGames, setExpandedGames] = useState<
    Record<string, boolean>
  >({});

  const {
    data,
    error,
    isLoading,
    fetchNextPage,
    hasNextPage,
    isFetchingNextPage,
  } = useLivePropsInfinite();

  /* ---------------------------------
     FLATTEN PAGES
  ---------------------------------- */
  const allRows = useMemo(() => {
    return data?.pages.flatMap((p) => p.items) ?? [];
  }, [data]);

  /* ---------------------------------
     GUARDRAIL + FILTER
  ---------------------------------- */
  const filteredRows = useMemo(() => {
    const capped =
      allRows.length <= MAX_ROWS
        ? allRows
        : allRows.slice(0, MAX_ROWS);

    return applyFilters(capped, filters);
  }, [allRows, filters]);

  /* ---------------------------------
     GROUP BY GAME
  ---------------------------------- */
  const groupedGames = useMemo(() => {
    const grouped = new Map<string, GameGroup>();

    for (const item of filteredRows) {
      const gameKey = resolveGameKey(item);
      const startTime = resolveGameStartTime(item);
      const existing = grouped.get(gameKey);

      if (!existing) {
        grouped.set(gameKey, {
          gameKey,
          gameId: item.game_id,
          home_team_abbr: item.home_team_abbr,
          away_team_abbr: item.away_team_abbr,
          game_period: item.game_period,
          game_clock: item.game_clock,
          game_state: item.game_state,
          startTimeMs: startTime,
          props: [item],
        });
      } else {
        existing.props.push(item);

        if (!existing.home_team_abbr && item.home_team_abbr) {
          existing.home_team_abbr = item.home_team_abbr;
        }
        if (!existing.away_team_abbr && item.away_team_abbr) {
          existing.away_team_abbr = item.away_team_abbr;
        }
        if (!existing.game_period && item.game_period) {
          existing.game_period = item.game_period;
        }
        if (!existing.game_clock && item.game_clock) {
          existing.game_clock = item.game_clock;
        }
        if (!existing.game_state && item.game_state) {
          existing.game_state = item.game_state;
        }
        if (existing.startTimeMs == null && startTime != null) {
          existing.startTimeMs = startTime;
        }
      }
    }

    const list = Array.from(grouped.values());

    for (const group of list) {
      group.props.sort((a, b) => {
        const oddsA = a.display_odds ?? Number.POSITIVE_INFINITY;
        const oddsB = b.display_odds ?? Number.POSITIVE_INFINITY;
        return oddsA - oddsB;
      });
    }

    list.sort((a, b) => {
      const timeA = a.startTimeMs ?? Number.POSITIVE_INFINITY;
      const timeB = b.startTimeMs ?? Number.POSITIVE_INFINITY;
      if (timeA !== timeB) return timeA - timeB;
      return a.gameKey.localeCompare(b.gameKey);
    });

    return list;
  }, [filteredRows]);

  const toggleGame = useCallback((gameKey: string) => {
    setExpandedGames((prev) => ({
      ...prev,
      [gameKey]: !prev[gameKey],
    }));
  }, []);

  /* ---------------------------------
     STATES
  ---------------------------------- */
  if (isLoading) {
    return (
      <View
        style={[
          styles.center,
          { backgroundColor: colors.surface.screen },
        ]}
      >
        <Text style={{ color: colors.text.muted }}>
          Loading live props…
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
        <Text style={{ color: colors.accent.danger }}>
          Error loading live props
        </Text>
      </View>
    );
  }

  if (!groupedGames || groupedGames.length === 0) {
    return (
      <View
        style={[
          styles.center,
          { backgroundColor: colors.surface.screen },
        ]}
      >
        <Text style={{ color: colors.text.muted }}>
          No live props available
        </Text>
      </View>
    );
  }

  /* ---------------------------------
     RENDER
  ---------------------------------- */
  return (
    <View
      style={[
        styles.container,
        { backgroundColor: colors.surface.screen },
      ]}
    >
      {/* FILTER BAR */}
      <LivePropFilterBar
        filters={filters}
        setFilters={setFilters}
      />

      {/* LIST */}
      <FlatList
        data={groupedGames}
        keyExtractor={(item) => item.gameKey}
        contentContainerStyle={{ padding: 12 }}
        renderItem={({ item }) => {
          const isExpanded = expandedGames[item.gameKey] ?? false;
          const awayAbbr = item.away_team_abbr ?? "AWAY";
          const homeAbbr = item.home_team_abbr ?? "HOME";
          const statusText = formatGameStatus(
            item.game_period,
            item.game_clock,
            item.game_state
          );

          return (
            <View
              style={[
                styles.gameCard,
                {
                  backgroundColor: colors.surface.card,
                  shadowColor: "#000",
                },
              ]}
            >
              <Pressable
                onPress={() => toggleGame(item.gameKey)}
                style={styles.gameHeaderRow}
              >
                <View style={styles.gameHeaderLeft}>
                  <View style={styles.matchupRow}>
                    {TEAM_LOGOS[awayAbbr] ? (
                      <Image
                        source={{ uri: TEAM_LOGOS[awayAbbr] }}
                        style={styles.teamLogo}
                      />
                    ) : (
                      <Text
                        style={[
                          styles.teamFallback,
                          { color: colors.text.primary },
                        ]}
                      >
                        {awayAbbr}
                      </Text>
                    )}

                    <Text
                      style={[
                        styles.vsText,
                        { color: colors.text.muted },
                      ]}
                    >
                      @
                    </Text>

                    {TEAM_LOGOS[homeAbbr] ? (
                      <Image
                        source={{ uri: TEAM_LOGOS[homeAbbr] }}
                        style={styles.teamLogo}
                      />
                    ) : (
                      <Text
                        style={[
                          styles.teamFallback,
                          { color: colors.text.primary },
                        ]}
                      >
                        {homeAbbr}
                      </Text>
                    )}
                  </View>

                  <Text
                    style={[
                      styles.matchupText,
                      { color: colors.text.primary },
                    ]}
                  >
                    {awayAbbr} @ {homeAbbr}
                  </Text>

                  <Text
                    style={[
                      styles.statusText,
                      { color: colors.text.secondary },
                    ]}
                  >
                    {statusText}
                  </Text>
                </View>

                <View style={styles.gameHeaderRight}>
                  <View
                    style={[
                      styles.propsPill,
                      { backgroundColor: colors.surface.elevated },
                    ]}
                  >
                    <Text
                      style={[
                        styles.propsPillText,
                        { color: colors.text.primary },
                      ]}
                    >
                      {item.props.length} props
                    </Text>
                  </View>

                  <Text
                    style={[
                      styles.expandText,
                      { color: colors.text.muted },
                    ]}
                  >
                    {isExpanded ? "v" : ">"}
                  </Text>
                </View>
              </Pressable>

              {isExpanded && (
                <View style={styles.propsContainer}>
                  {item.props.map((prop) => (
                    <LivePropCard
                      key={
                        prop.prop_key ??
                        `${prop.player_id}-${prop.market}-${prop.line}-${prop.book}`
                      }
                      item={prop}
                    />
                  ))}
                </View>
              )}
            </View>
          );
        }}
        onEndReached={() => {
          if (hasNextPage && !isFetchingNextPage) {
            fetchNextPage();
          }
        }}
        onEndReachedThreshold={0.6}
        ListFooterComponent={
          isFetchingNextPage ? (
            <Text
              style={{
                textAlign: "center",
                paddingVertical: 12,
                color: colors.text.muted,
              }}
            >
              Loading more…
            </Text>
          ) : null
        }
      />
    </View>
  );
}

/* ======================================================
   STYLES
====================================================== */

const styles = StyleSheet.create({
  container: {
    flex: 1,
  },

  gameCard: {
    padding: 14,
    borderRadius: 18,
    marginBottom: 14,
    shadowOpacity: 0.04,
    shadowRadius: 14,
    shadowOffset: { width: 0, height: 6 },
  },

  gameHeaderRow: {
    flexDirection: "row",
    justifyContent: "space-between",
    gap: 12,
  },

  gameHeaderLeft: {
    flex: 1,
  },

  gameHeaderRight: {
    alignItems: "flex-end",
    gap: 8,
  },

  matchupRow: {
    flexDirection: "row",
    alignItems: "center",
    gap: 8,
  },

  teamLogo: {
    width: 26,
    height: 26,
  },

  teamFallback: {
    fontSize: 12,
    fontWeight: "800",
  },

  vsText: {
    fontSize: 12,
    fontWeight: "800",
  },

  matchupText: {
    marginTop: 6,
    fontSize: 14,
    fontWeight: "900",
  },

  statusText: {
    marginTop: 2,
    fontSize: 12,
    fontWeight: "700",
  },

  propsPill: {
    paddingHorizontal: 10,
    paddingVertical: 4,
    borderRadius: 999,
  },

  propsPillText: {
    fontSize: 11,
    fontWeight: "800",
  },

  expandText: {
    fontSize: 14,
    fontWeight: "900",
  },

  propsContainer: {
    marginTop: 10,
  },

  center: {
    flex: 1,
    alignItems: "center",
    justifyContent: "center",
  },
});