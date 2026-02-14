// app/(tabs)/props
import {
  View,
  Text,
  StyleSheet,
  FlatList,
  SectionList,
  Pressable,
  Image,
} from "react-native";
import { useMemo, useState, useCallback, useRef } from "react";
import Slider from "@react-native-community/slider";
import { GestureHandlerRootView } from "react-native-gesture-handler";
import * as Haptics from "expo-haptics";
import { useRouter } from "expo-router";

import PropCard from "@/components/PropCard";
import { useTheme } from "@/store/useTheme";
import { useSavedBets } from "@/store/useSavedBets";
import { useHistoricalPlayerTrends } from "@/hooks/useHistoricalPlayerTrends";
import { resolveSparklineByMarket } from "@/utils/resolveSparkline";
import { usePlayerPropsMaster } from "@/hooks/usePlayerPropsMaster";
import { useBetslipDrawer } from "@/store/useBetslipDrawer";
import { usePropBetslip } from "@/store/usePropBetslip";
import { normalizeMarket } from "@/utils/normalizeMarket";
import { useBadLineAlerts } from "@/hooks/useBadLineAlerts";
import { useWowy } from "@/hooks/useWowy";
import {
  OpponentPositionDefenseRow,
  useOpponentPositionDefense,
} from "@/hooks/useOpponentPositionDefense";
import {
  PlayerPositionRow,
  usePlayerPositions,
} from "@/hooks/usePlayerPositions";
import { InjuredPlayerWowy, TeammateWowy, WowyStat } from "@/lib/wowy";
import { TEAM_LOGOS } from "@/utils/teamLogos";

function getOpponentRank(
  row: OpponentPositionDefenseRow | undefined,
  market?: string,
) {
  const normalizedMarket = normalizeMarketKey(market);
  if (!row || !normalizedMarket) return undefined;
  switch (normalizedMarket) {
    case "pts":
      return row.pts_allowed_rank;
    case "reb":
      return row.reb_allowed_rank;
    case "ast":
      return row.ast_allowed_rank;
    case "stl":
      return row.stl_allowed_rank;
    case "blk":
      return row.blk_allowed_rank;
    case "fg3m":
      return row.fg3m_allowed_rank;
    case "pa":
      return row.pa_allowed_rank;
    case "pr":
      return row.pr_allowed_rank;
    case "pra":
      return row.pra_allowed_rank;
    case "dd":
      return row.dd_rate_allowed_rank;
    case "td":
      return row.td_rate_allowed_rank;
    default:
      return undefined;
  }
}

function getOpponentAllowedStat(
  row: OpponentPositionDefenseRow | undefined,
  market?: string,
) {
  const normalizedMarket = normalizeMarketKey(market);
  if (!row || !normalizedMarket) return undefined;
  switch (normalizedMarket) {
    case "pts":
      return { value: row.pts_allowed_avg, isRate: false };
    case "reb":
      return { value: row.reb_allowed_avg, isRate: false };
    case "ast":
      return { value: row.ast_allowed_avg, isRate: false };
    case "stl":
      return { value: row.stl_allowed_avg, isRate: false };
    case "blk":
      return { value: row.blk_allowed_avg, isRate: false };
    case "fg3m":
      return { value: row.fg3m_allowed_avg, isRate: false };
    case "pa":
      return { value: row.pa_allowed_avg, isRate: false };
    case "pr":
      return { value: row.pr_allowed_avg, isRate: false };
    case "pra":
      return { value: row.pra_allowed_avg, isRate: false };
    case "dd":
      return { value: row.dd_rate_allowed, isRate: true };
    case "td":
      return { value: row.td_rate_allowed, isRate: true };
    default:
      return undefined;
  }
}

function normalizeMarketKey(value?: string) {
  if (!value) return undefined;
  const key = value
    .toLowerCase()
    .replace(/\s+/g, "")
    .replace(/_/g, "");
  const altMarketMap: Record<string, string> = {
    playerpoints: "pts",
    playerpointsalternate: "pts",
    pointsalternate: "pts",
    playerrebounds: "reb",
    playerreboundsalternate: "reb",
    reboundsalternate: "reb",
    playerassists: "ast",
    playerassistsalternate: "ast",
    assistsalternate: "ast",
    playerthrees: "fg3m",
    playerthreesalternate: "fg3m",
    threesalternate: "fg3m",
    playerpointsreboundsassists: "pra",
    playerpointsreboundsassistsalternate: "pra",
    pointsreboundsassistsalternate: "pra",
    playerpointsrebounds: "pr",
    playerpointsreboundsalternate: "pr",
    pointsreboundsalternate: "pr",
    playerpointsassists: "pa",
    playerpointsassistsalternate: "pa",
    pointsassistsalternate: "pa",
    playerreboundsassists: "ra",
    playerreboundsassistsalternate: "ra",
    reboundsassistsalternate: "ra",
  };
  const alt = altMarketMap[key];
  if (alt) return alt;
  if (["pts", "point", "points"].includes(key)) return "pts";
  if (["reb", "rebound", "rebounds"].includes(key)) return "reb";
  if (["ast", "assist", "assists"].includes(key)) return "ast";
  if (["stl", "steal", "steals"].includes(key)) return "stl";
  if (["blk", "block", "blocks"].includes(key)) return "blk";
  if (
    [
      "3pm",
      "3pt",
      "3pts",
      "3pointersmade",
      "threepointersmade",
      "fg3m",
    ].includes(key)
  ) {
    return "fg3m";
  }
  if (["pa", "pointsassists"].includes(key)) return "pa";
  if (["pr", "pointsrebounds"].includes(key)) return "pr";
  if (["ra", "reboundsassists"].includes(key)) return "ra";
  if (["pra", "pointsreboundsassists"].includes(key)) return "pra";
  if (["dd", "doubledouble"].includes(key)) return "dd";
  if (["td", "tripledouble"].includes(key)) return "td";
  return key;
}

function resolveWowyStat(market?: string): WowyStat | null {
  const key = normalizeMarketKey(market);
  if (key === "pts") return "pts";
  if (key === "reb") return "reb";
  if (key === "ast") return "ast";
  if (key === "fg3m") return "fg3m";
  return null;
}

function getWowyStatValues(tm: TeammateWowy, stat: WowyStat) {
  switch (stat) {
    case "pts":
      return {
        diff: tm.pts_diff,
        withStat: tm.pts_with,
        withoutStat: tm.pts_without,
        label: "PTS",
      };
    case "reb":
      return {
        diff: tm.reb_diff,
        withStat: tm.reb_with,
        withoutStat: tm.reb_without,
        label: "REB",
      };
    case "ast":
      return {
        diff: tm.ast_diff,
        withStat: tm.ast_with,
        withoutStat: tm.ast_without,
        label: "AST",
      };
    case "fg3m":
      return {
        diff: tm.fg3m_diff,
        withStat: tm.fg3m_with,
        withoutStat: tm.fg3m_without,
        label: "3PM",
      };
  }
}

function getWowyLabel(stat: WowyStat) {
  switch (stat) {
    case "pts":
      return "PTS";
    case "reb":
      return "REB";
    case "ast":
      return "AST";
    case "fg3m":
      return "3PM";
  }
}

type WowyImpactEntry = {
  injuredPlayer: InjuredPlayerWowy["injured_player"];
  teammate: TeammateWowy;
  gamesWith: number;
  gamesWithout: number;
};

function normalizePosition(value?: string) {
  return value?.trim().toUpperCase();
}

function resolveOpponentTeamAbbr({
  playerTeamAbbr,
  homeTeam,
  awayTeam,
}: {
  playerTeamAbbr?: string;
  homeTeam?: string;
  awayTeam?: string;
}) {
  if (!playerTeamAbbr || !homeTeam || !awayTeam) return undefined;
  if (playerTeamAbbr === homeTeam) return awayTeam;
  if (playerTeamAbbr === awayTeam) return homeTeam;
  return undefined;
}

function resolveGameLabel(item: {
  homeTeam?: string;
  awayTeam?: string;
  matchup?: string;
  game_matchup?: string;
  matchup_display?: string;
}) {
  if (item.awayTeam && item.homeTeam) {
    return `${item.awayTeam} @ ${item.homeTeam}`;
  }

  const fallback =
    item.matchup ?? item.game_matchup ?? item.matchup_display ?? "";
  const trimmed = typeof fallback === "string" ? fallback.trim() : "";

  return trimmed || "Other";
}

function normalizeTeamKey(team?: string) {
  if (!team) return undefined;
  const key = team.trim().toUpperCase();
  if (key === "NO") return "NOP";
  if (key === "PHO") return "PHX";
  return key;
}

function normalizeBookmakerKey(raw?: string) {
  if (!raw) return null;
  const key = raw.toLowerCase().replace(/[\s_]/g, "");
  if (key.startsWith("draft")) return "draftkings";
  if (key.startsWith("fan")) return "fanduel";
  if (key === "dk") return "draftkings";
  if (key === "fd") return "fanduel";
  return key;
}

function resolveTeamLogo(team?: string) {
  const key = normalizeTeamKey(team);
  return key ? TEAM_LOGOS[key] : undefined;
}

function formatGameStartTime(startTimeMs?: number | null) {
  if (!startTimeMs) return "TBD";
  const date = new Date(startTimeMs);
  if (Number.isNaN(date.getTime())) return "TBD";
  return date.toLocaleTimeString([], { hour: "numeric", minute: "2-digit" });
}

/* ======================================================
   Screen
====================================================== */
export default function PropsTestScreen() {
  const colors = useTheme((s) => s.colors);
  const styles = useMemo(() => makeStyles(colors), [colors]);
  const router = useRouter();

  const savedIds = useSavedBets((s) => s.savedIds);
  const toggleSave = useSavedBets((s) => s.toggleSave);
  const openBetslip = useBetslipDrawer((s) => s.open);
  const isBetslipOpen = useBetslipDrawer((s) => s.isOpen);
  const addToBetslip = usePropBetslip((s) => s.add);
  const removeFromBetslip = usePropBetslip((s) => s.remove);
  const [filtersOpen, setFiltersOpen] = useState(false);
  const [activeSubTab, setActiveSubTab] = useState<"ALL" | "BY_GAME">("ALL");
  const [sortOption, setSortOption] = useState<
    "HIT_RATE" | "ODDS" | "BAD_LINES"
  >("HIT_RATE");
  const [expandedGameKey, setExpandedGameKey] = useState<string | null>(null);


  const {
    props: rawProps,
    loading,
    filters,
    setFilters,
  } = usePlayerPropsMaster();
  const { data: opponentPositionRows } = useOpponentPositionDefense();
  const { data: playerPositions } = usePlayerPositions();
  const { injuredPlayers: wowyPlayers } = useWowy();

  const { getByPlayer } = useHistoricalPlayerTrends();

  const listRef = useRef<FlatList>(null);
  const gameListRef = useRef<SectionList>(null);
  const [expandedId, setExpandedId] = useState<string | null>(null);

  const toggleExpand = useCallback((id: string) => {
    setExpandedId((prev) => (prev === id ? null : id));
  }, []);

  const toggleGame = useCallback((key: string) => {
    setExpandedGameKey((prev) => {
      const next = prev === key ? null : key;

      if (next) {
        requestAnimationFrame(() => {
          gameListRef.current?.scrollToOffset({
            offset: 0,
            animated: true,
          });
        });
      }

      return next;
    });
  }, []);

  const { data: badLines } = useBadLineAlerts(1.0);

  const badLineMap = useMemo(() => {
    const map = new Map<number, number>();

    badLines?.forEach((b) => {
      map.set(b.prop_id, b.bad_line_score);
    });

    return map;
  }, [badLines]);

  /* ======================================================
     SAVE / UNSAVE (single source of truth)
  ====================================================== */
  const saveProp = useCallback(
    (item: any) => {
      if (savedIds.has(item.id)) return;
  
      toggleSave(item.id);
  
      addToBetslip({
        id: item.id,

        player_id: item.player_id,          // 🔥 REQUIRED
        player: item.player,

        market: normalizeMarket(item.market), // 🔥 REQUIRED
        side: item.side ?? "over",
        line: item.line,
        odds: item.odds,

        matchup: item.matchup,
      });

  
      Haptics.impactAsync(Haptics.ImpactFeedbackStyle.Medium);
  
      // ✅ OPEN THE DRAWER
      openBetslip();
    },
    [savedIds, toggleSave, addToBetslip, openBetslip]
  );

  const unsaveProp = useCallback(
    (id: string) => {
      toggleSave(id);
      removeFromBetslip(id);
    },
    [toggleSave, removeFromBetslip]
  );

  /* ======================================================
     SANITIZE + FILTER + SORT
  ====================================================== */
  const props = useMemo(() => {
    const allowedBooks = new Set(["draftkings", "fanduel"]);
    const cleaned = rawProps
      .filter((p) => {
        if (!p.player) return false;
        if (!p.market) return false;
        if (p.line == null) return false;
        if (!p.id) return false;

        const bookKey = normalizeBookmakerKey(p.bookmaker);
        if (!bookKey || !allowedBooks.has(bookKey)) return false;

        const hitRate =
          filters.hitRateWindow === "L5"
            ? p.hit_rate_l5
            : filters.hitRateWindow === "L10"
            ? p.hit_rate_l10
            : p.hit_rate_l20;

        const hitRatePct =
          hitRate != null && hitRate <= 1 ? hitRate * 100 : hitRate;

        if (hitRatePct != null && hitRatePct < filters.minHitRate) {
          return false;
        }

        if (p.odds < filters.minOdds) return false;
        if (p.odds > filters.maxOdds) return false;

        return true;
      });

    const groupedMap = new Map<
      string,
      {
        base: any;
        books: { bookmaker: string; odds: number }[];
      }
    >();

    cleaned.forEach((p) => {
      const bookKey = normalizeBookmakerKey(p.bookmaker);
      if (!bookKey) return;
      const marketKey = normalizeMarket(p.market);
      const sideKey = p.side ?? "over";
      const groupKey = `${p.player_id ?? p.player}::${marketKey}::${p.line}::${sideKey}`;
      const existing = groupedMap.get(groupKey);
      if (!existing) {
        groupedMap.set(groupKey, {
          base: {
            ...p,
            market: marketKey,
            id: groupKey,
          },
          books: [{ bookmaker: bookKey, odds: p.odds }],
        });
        return;
      }

      const existingBook = existing.books.find(
        (book) => book.bookmaker === bookKey,
      );
      if (existingBook) {
        if (p.odds > existingBook.odds) {
          existingBook.odds = p.odds;
        }
        return;
      }

      existing.books.push({ bookmaker: bookKey, odds: p.odds });
    });

    const grouped = Array.from(groupedMap.values()).map((entry) => {
      const sortedBooks = [...entry.books].sort((a, b) => {
        const order = ["draftkings", "fanduel"];
        const aIndex = order.indexOf(a.bookmaker);
        const bIndex = order.indexOf(b.bookmaker);
        return (aIndex === -1 ? 99 : aIndex) - (bIndex === -1 ? 99 : bIndex);
      });
      const bestBook = sortedBooks.reduce((best, current) =>
        current.odds > best.odds ? current : best,
      );
      return {
        ...entry.base,
        odds: bestBook.odds,
        bookmaker: bestBook.bookmaker,
        bookOdds: sortedBooks,
      };
    });

    const getHitRateValue = (prop: any) =>
      filters.hitRateWindow === "L5"
        ? prop.hit_rate_l5
        : filters.hitRateWindow === "L10"
        ? prop.hit_rate_l10
        : prop.hit_rate_l20;

    grouped.sort((a, b) => {
      if (sortOption === "ODDS") {
        const oddsDiff = (a.odds ?? 0) - (b.odds ?? 0);
        if (oddsDiff !== 0) return oddsDiff;
        return (a.player ?? "").localeCompare(b.player ?? "");
      }

      if (sortOption === "BAD_LINES") {
        const aScore = badLineMap.get(a.propId) ?? 0;
        const bScore = badLineMap.get(b.propId) ?? 0;

        const aIsBadLine = aScore > 0;
        const bIsBadLine = bScore > 0;

        if (aIsBadLine && !bIsBadLine) return -1;
        if (!aIsBadLine && bIsBadLine) return 1;
        if (aScore !== bScore) return bScore - aScore;
      }

      const aHitRate = getHitRateValue(a) ?? 0;
      const bHitRate = getHitRateValue(b) ?? 0;
      const hitRateDiff = bHitRate - aHitRate;
      if (hitRateDiff !== 0) return hitRateDiff;
      return (a.player ?? "").localeCompare(b.player ?? "");
    });

    return grouped;
  }, [rawProps, filters, sortOption, badLineMap]);

  const gameGroups = useMemo(() => {
    const map = new Map<
      string,
      {
        key: string;
        label: string;
        items: any[];
        homeTeam?: string;
        awayTeam?: string;
        startTimeMs?: number | null;
      }
    >();

    props.forEach((prop) => {
      const label = resolveGameLabel(prop);
      const existing = map.get(label);
      const startTimeMs = prop.startTimeMs ?? null;

      if (!existing) {
        map.set(label, {
          key: label,
          label,
          items: [prop],
          homeTeam: prop.homeTeam,
          awayTeam: prop.awayTeam,
          startTimeMs,
        });
        return;
      }

      existing.items.push(prop);

      if (!existing.homeTeam && prop.homeTeam) {
        existing.homeTeam = prop.homeTeam;
      }
      if (!existing.awayTeam && prop.awayTeam) {
        existing.awayTeam = prop.awayTeam;
      }
      if (existing.startTimeMs == null && startTimeMs != null) {
        existing.startTimeMs = startTimeMs;
      }
    });

    const groups = Array.from(map.values()).map((group) => ({
      ...group,
      items: [...group.items].sort((a, b) => {
        if (sortOption === "ODDS") {
          const oddsDiff = (a.odds ?? 0) - (b.odds ?? 0);
          if (oddsDiff !== 0) return oddsDiff;
          return (a.player ?? "").localeCompare(b.player ?? "");
        }

        if (sortOption === "BAD_LINES") {
          const aScore = badLineMap.get(a.propId) ?? 0;
          const bScore = badLineMap.get(b.propId) ?? 0;

          const aIsBadLine = aScore > 0;
          const bIsBadLine = bScore > 0;

          if (aIsBadLine && !bIsBadLine) return -1;
          if (!aIsBadLine && bIsBadLine) return 1;
          if (aScore !== bScore) return bScore - aScore;
        }

        const aHitRate =
          filters.hitRateWindow === "L5"
            ? a.hit_rate_l5
            : filters.hitRateWindow === "L10"
            ? a.hit_rate_l10
            : a.hit_rate_l20;
        const bHitRate =
          filters.hitRateWindow === "L5"
            ? b.hit_rate_l5
            : filters.hitRateWindow === "L10"
            ? b.hit_rate_l10
            : b.hit_rate_l20;
        const hitRateDiff = (bHitRate ?? 0) - (aHitRate ?? 0);
        if (hitRateDiff !== 0) return hitRateDiff;
        return (a.player ?? "").localeCompare(b.player ?? "");
      }),
    }));

    groups.sort((a, b) => {
      if (a.key === "Other") return 1;
      if (b.key === "Other") return -1;
      return a.key.localeCompare(b.key);
    });

    if (!expandedGameKey) {
      return groups;
    }

    const expandedGroup = groups.find(
      (group) => group.key === expandedGameKey,
    );

    if (!expandedGroup) {
      return groups;
    }

    return [
      expandedGroup,
      ...groups.filter((group) => group.key !== expandedGameKey),
    ];
  }, [props, sortOption, badLineMap, filters.hitRateWindow, expandedGameKey]);

  const listContentContainerStyle = useMemo(
    () => [
      styles.list,
      { paddingBottom: isBetslipOpen ? 320 : 120 },
    ],
    [styles.list, isBetslipOpen],
  );

  const opponentPositionMap = useMemo(() => {
    const map = new Map<string, OpponentPositionDefenseRow>();
    opponentPositionRows.forEach((row) => {
      const position = normalizePosition(row.player_position);
      if (!position) return;
      map.set(`${row.opponent_team_abbr}-${position}`, row);
    });
    return map;
  }, [opponentPositionRows]);

  const playerPositionMap = useMemo(() => {
    const map = new Map<number, PlayerPositionRow>();
    playerPositions.forEach((row) => {
      if (row.player_id != null && row.position && row.team_abbr) {
        map.set(row.player_id, row);
      }
    });
    return map;
  }, [playerPositions]);

  const wowyImpactMap = useMemo(() => {
    const map = new Map<number, WowyImpactEntry[]>();

    wowyPlayers.forEach((injured) => {
      const injuredInfo = injured.injured_player;
      injured.teammates.forEach((teammate) => {
        const entry: WowyImpactEntry = {
          injuredPlayer: injuredInfo,
          teammate,
          gamesWith: teammate.games_with,
          gamesWithout: teammate.games_without,
        };
        const existing = map.get(teammate.player_id) ?? [];
        existing.push(entry);
        map.set(teammate.player_id, existing);
      });
    });

    return map;
  }, [wowyPlayers]);

  /* ======================================================
     RENDER ITEM
  ====================================================== */
  const renderPropCard = useCallback(
    (item: any, scrollRef: React.RefObject<FlatList<any>>) => {
      const trend = getByPlayer(item.player);
      const spark = resolveSparklineByMarket(item.market, trend);
      const isSaved = savedIds.has(item.id);
      const playerLookup = playerPositionMap.get(item.player_id);
      const playerPosition = normalizePosition(
        playerLookup?.position ?? item.player_position,
      );
      const playerTeamAbbr =
        item.playerTeamAbbr ??
        playerLookup?.team_abbr ??
        item.team_abbr;
      const opponentTeamAbbr =
        item.opponentTeamAbbr ??
        resolveOpponentTeamAbbr({
          playerTeamAbbr,
          homeTeam: item.homeTeam,
          awayTeam: item.awayTeam,
        });
      const opponentRow =
        opponentTeamAbbr && playerPosition
          ? opponentPositionMap.get(
              `${opponentTeamAbbr}-${playerPosition}`,
            )
          : undefined;
      const opponentPositionRank = getOpponentRank(
        opponentRow,
        item.market,
      );
      const opponentStatAllowed = getOpponentAllowedStat(
        opponentRow,
        item.market,
      );
      const wowyStat = resolveWowyStat(item.market);
      const wowyEntries =
        wowyStat && item.player_id != null
          ? wowyImpactMap.get(item.player_id) ?? []
          : [];
      const wowyImpacts =
        wowyStat && wowyEntries.length > 0
          ? [...wowyEntries]
              .map((entry) => {
                const values = getWowyStatValues(
                  entry.teammate,
                  wowyStat,
                );
                return {
                  injuredPlayerId: entry.injuredPlayer.player_id,
                  injuredPlayerName: entry.injuredPlayer.player_name,
                  injuredStatus: entry.injuredPlayer.status,
                  diff: values.diff,
                  statWith: values.withStat,
                  statWithout: values.withoutStat,
                  gamesWith: entry.gamesWith,
                  gamesWithout: entry.gamesWithout,
                };
              })
              .sort((a, b) => {
                const aDiff = a.diff ?? 0;
                const bDiff = b.diff ?? 0;
                return Math.abs(bDiff) - Math.abs(aDiff);
              })
          : wowyStat
            ? []
            : undefined;
      const paceValue =
        filters.hitRateWindow === "L5"
          ? item.pace_l5
          : filters.hitRateWindow === "L20"
          ? item.pace_l20
          : item.pace_l10;
      const usageValue =
        filters.hitRateWindow === "L5"
          ? item.usage_l5
          : filters.hitRateWindow === "L20"
          ? item.usage_l20
          : item.usage_l10;

      return (
        <PropCard
          {...item}
          bookmaker={item.bookmaker}
          bookOdds={item.bookOdds}
          playerId={item.player_id}
          scrollRef={scrollRef}
          saved={isSaved}
          badLineScore={badLineMap.get(item.propId)}
          playerPosition={playerPosition}
          opponentTeamAbbr={opponentTeamAbbr}
          opponentPositionRank={opponentPositionRank}
          teamPace={paceValue}
          usagePct={usageValue}
          opponentStatAllowed={opponentStatAllowed?.value}
          opponentStatAllowedIsRate={opponentStatAllowed?.isRate}
          wowyStatLabel={wowyStat ? getWowyLabel(wowyStat) : undefined}
          wowyImpacts={wowyImpacts}
          onSwipeSave={() => saveProp(item)}
          onToggleSave={() =>
            isSaved ? unsaveProp(item.id) : saveProp(item)
          }
          expanded={expandedId === item.id}
          onToggleExpand={() => toggleExpand(item.id)}
          sparkline_l5={spark?.sparkline_l5}
          sparkline_l10={spark?.sparkline_l10}
          sparkline_l20={spark?.sparkline_l20}
          last5_dates={trend?.last5_dates}
          last10_dates={trend?.last10_dates}
          last20_dates={trend?.last20_dates}
          window={filters.hitRateWindow}
          avg_l5={item.avg_l5}
          avg_l10={item.avg_l10}
          avg_l20={item.avg_l20}
        />
      );
    },
    [
      savedIds,
      expandedId,
      toggleExpand,
      getByPlayer,
      saveProp,
      unsaveProp,
      opponentPositionMap,
      playerPositionMap,
      filters.hitRateWindow,
      badLineMap,
      wowyImpactMap,
    ]
  );

  const renderItem = useCallback(
    ({ item }: any) => renderPropCard(item, listRef),
    [renderPropCard, listRef]
  );

  const renderGameHeader = useCallback(
    ({
      section,
    }: {
      section: {
        key: string;
        label: string;
        data: any[];
        itemCount: number;
        homeTeam?: string;
        awayTeam?: string;
        startTimeMs?: number | null;
      };
    }) => {
      const item = section;
      const isExpanded = expandedGameKey === item.key;
      const awayLogo = resolveTeamLogo(item.awayTeam);
      const homeLogo = resolveTeamLogo(item.homeTeam);
      const startTimeLabel = formatGameStartTime(item.startTimeMs);
      const gameMetaText = `${startTimeLabel} | ${item.itemCount} props`;

      return (
        <View style={styles.gameGroup}>
          <Pressable
            style={styles.gameHeader}
            onPress={() => toggleGame(item.key)}
          >
            <View style={styles.gameHeaderLeft}>
              <View style={styles.gameMatchupRow}>
                {(awayLogo || homeLogo) && (
                  <View style={styles.matchupLogoRow}>
                    {awayLogo ? (
                      <Image
                        source={{ uri: awayLogo }}
                        style={styles.gameTeamLogo}
                      />
                    ) : null}
                    {awayLogo && homeLogo ? (
                      <Text style={styles.gameAtSymbol}>@</Text>
                    ) : null}
                    {homeLogo ? (
                      <Image
                        source={{ uri: homeLogo }}
                        style={styles.gameTeamLogo}
                      />
                    ) : null}
                  </View>
                )}
                <Text style={styles.gameTitle}>{item.label}</Text>
              </View>
              <Text style={styles.gameMeta}>{gameMetaText}</Text>
            </View>
            <Text style={styles.gameChevron}>
              {isExpanded ? "▲" : "▼"}
            </Text>
          </Pressable>
        </View>
      );
    },
    [
      expandedGameKey,
      styles.gameGroup,
      styles.gameHeader,
      styles.gameHeaderLeft,
      styles.gameMatchupRow,
      styles.matchupLogoRow,
      styles.gameTeamLogo,
      styles.gameAtSymbol,
      styles.gameTitle,
      styles.gameMeta,
      styles.gameChevron,
      toggleGame,
    ]
  );

  const gameSections = useMemo(
    () =>
      gameGroups.map((group) => ({
        key: group.key,
        label: group.label,
        homeTeam: group.homeTeam,
        awayTeam: group.awayTeam,
        startTimeMs: group.startTimeMs,
        itemCount: group.items.length,
        data: expandedGameKey === group.key ? group.items : [],
      })),
    [gameGroups, expandedGameKey],
  );

  if (loading) {
    return (
      <GestureHandlerRootView style={styles.root}>
        <View style={styles.center}>
          <Text style={styles.loading}>Loading test props…</Text>
        </View>
      </GestureHandlerRootView>
    );
  }

  /* ======================================================
     UI
  ====================================================== */
  return (
    <GestureHandlerRootView style={styles.root}>
      <View style={styles.screen}>
        <View style={styles.filters}>
          {/* FILTER HEADER */}
          <Pressable
            style={styles.filterHeader}
            onPress={() => setFiltersOpen((v) => !v)}
          >
            <Text style={styles.filtersTitle}>Filters</Text>
            <Text style={styles.chevron}>
              {filtersOpen ? "▲" : "▼"}
            </Text>
          </Pressable>

          {/* FILTER BODY */}
          {filtersOpen && (
            <View style={styles.filterBody}>
              {/* MARKET FILTERS */}
              <View style={styles.pills}>
                {filters.markets.map((mkt) => {
                  const active = filters.market === mkt;
                  return (
                    <Pressable
                      key={mkt}
                      onPress={() =>
                        setFilters((f) => ({
                          ...f,
                          market: active ? "ALL" : mkt,
                        }))
                      }
                    >
                      <Text
                        style={[
                          styles.pill,
                          active && styles.pillActive,
                        ]}
                      >
                        {mkt}
                      </Text>
                    </Pressable>
                  );
                })}
              </View>

              {/* WINDOW FILTER */}
              <Text style={styles.sectionLabel}>Market Window</Text>
              <View style={styles.pills}>
                {(["FULL", "Q1", "FIRST3MIN"] as const).map((w) => {
                  const active = filters.marketWindow === w;
                  return (
                    <Pressable
                      key={w}
                      onPress={() =>
                        setFilters((f) => ({
                          ...f,
                          marketWindow: active ? null : w,
                        }))
                      }
                    >
                      <Text
                        style={[
                          styles.pill,
                          active && styles.pillActive,
                        ]}
                      >
                        {w}
                      </Text>
                    </Pressable>
                  );
                })}
              </View>

              {/* SORT */}
              <Text style={styles.sectionLabel}>Sort</Text>
              <View style={styles.pills}>
                {[
                  { value: "HIT_RATE", label: "Hit Rate" },
                  { value: "ODDS", label: "Odds" },
                  { value: "BAD_LINES", label: "Bad Lines" },
                ].map((option) => {
                  const active = sortOption === option.value;
                  return (
                    <Pressable
                      key={option.value}
                      onPress={() =>
                        setSortOption(
                          option.value as
                            | "HIT_RATE"
                            | "ODDS"
                            | "BAD_LINES"
                        )
                      }
                    >
                      <Text
                        style={[
                          styles.pill,
                          active && styles.pillActive,
                        ]}
                      >
                        {option.label}
                      </Text>
                    </Pressable>
                  );
                })}
              </View>

              {/* HIT RATE WINDOW FILTER */}
              <Text style={styles.sectionLabel}>Hit Rate Window</Text>
              <View style={styles.pills}>
                {(["L5", "L10", "L20"] as const).map((w) => {
                  const active = filters.hitRateWindow === w;
                  return (
                    <Pressable
                      key={w}
                      onPress={() =>
                        setFilters((f) => ({
                          ...f,
                          hitRateWindow: w,
                        }))
                      }
                    >
                      <Text
                        style={[
                          styles.pill,
                          active && styles.pillActive,
                        ]}
                      >
                        {w}
                      </Text>
                    </Pressable>
                  );
                })}
              </View>

              {/* HIT RATE FILTER */}
              <Text style={styles.sectionLabel}>Min Hit Rate</Text>
              <Slider
                minimumValue={0}
                maximumValue={100}
                step={5}
                value={filters.minHitRate}
                onValueChange={(val) =>
                  setFilters((f) => ({
                    ...f,
                    minHitRate: val,
                  }))
                }
              />
              <Text style={styles.sliderValue}>
                {filters.minHitRate}%
              </Text>

              {/* ODDS FILTER */}
              <Text style={styles.sectionLabel}>Odds Range</Text>
              <Slider
                minimumValue={-1000}
                maximumValue={1000}
                step={25}
                value={filters.minOdds}
                onValueChange={(val) =>
                  setFilters((f) => ({
                    ...f,
                    minOdds: val,
                  }))
                }
              />
              <Text style={styles.sliderValue}>
                Min: {filters.minOdds}
              </Text>

              <Slider
                minimumValue={-1000}
                maximumValue={1000}
                step={25}
                value={filters.maxOdds}
                onValueChange={(val) =>
                  setFilters((f) => ({
                    ...f,
                    maxOdds: val,
                  }))
                }
              />
              <Text style={styles.sliderValue}>
                Max: {filters.maxOdds}
              </Text>
            </View>
          )}
        </View>

        <View style={styles.subTabs}>
          <Pressable
            style={[
              styles.subTab,
              activeSubTab === "ALL" && styles.subTabActive,
            ]}
            onPress={() => setActiveSubTab("ALL")}
          >
            <Text
              style={[
                styles.subTabText,
                activeSubTab === "ALL" && styles.subTabTextActive,
              ]}
            >
              All Props
            </Text>
          </Pressable>
          <Pressable
            style={[
              styles.subTab,
              activeSubTab === "BY_GAME" && styles.subTabActive,
            ]}
            onPress={() => setActiveSubTab("BY_GAME")}
          >
            <Text
              style={[
                styles.subTabText,
                activeSubTab === "BY_GAME" && styles.subTabTextActive,
              ]}
            >
              By Game
            </Text>
          </Pressable>
        </View>

        {activeSubTab === "ALL" ? (
          <FlatList
            ref={listRef}
            data={props}
            keyExtractor={(item) => item.id}
            renderItem={renderItem}
            contentContainerStyle={listContentContainerStyle}
          />
        ) : (
          <SectionList
            ref={gameListRef}
            sections={gameSections}
            keyExtractor={(item) => item.id}
            renderItem={({ item }) => renderPropCard(item, gameListRef)}
            renderSectionHeader={renderGameHeader}
            stickySectionHeadersEnabled
            contentContainerStyle={listContentContainerStyle}
          />
        )}
      </View>
    </GestureHandlerRootView>
  );
}

/* ======================================================
   STYLES
====================================================== */
const makeStyles = (colors: any) =>
  StyleSheet.create({
    root: {
      flex: 1,
    },
    screen: {
      flex: 1,
      backgroundColor: colors.surface.screen,
    },
    list: {
      paddingBottom: 40,
    },
    subTabs: {
      flexDirection: "row",
      paddingHorizontal: 14,
      borderBottomWidth: 1,
      borderBottomColor: colors.border.subtle,
    },
    subTab: {
      paddingVertical: 10,
      paddingHorizontal: 12,
      marginRight: 8,
      borderBottomWidth: 2,
      borderBottomColor: "transparent",
    },
    subTabActive: {
      borderBottomColor: colors.accent.primary,
    },
    subTabText: {
      fontSize: 14,
      fontWeight: "600",
      color: colors.text.muted,
    },
    subTabTextActive: {
      color: colors.text.primary,
    },
    filters: {
      padding: 14,
      borderBottomWidth: 1,
      borderBottomColor: colors.border.subtle,
    },
    filterHeader: {
      flexDirection: "row",
      justifyContent: "space-between",
      alignItems: "center",
    },
    filtersTitle: {
      fontSize: 16,
      fontWeight: "700",
      color: colors.text.primary,
    },
    chevron: {
      fontSize: 12,
      fontWeight: "700",
      color: colors.text.muted,
    },
    filterBody: {
      marginTop: 12,
    },
    sectionLabel: {
      fontSize: 12,
      fontWeight: "700",
      color: colors.text.muted,
      marginTop: 12,
      marginBottom: 4,
    },
    pills: {
      flexDirection: "row",
      flexWrap: "wrap",
      gap: 8,
    },
    pill: {
      backgroundColor: colors.surface.cardSoft,
      paddingHorizontal: 10,
      paddingVertical: 6,
      borderRadius: 12,
      fontSize: 12,
      fontWeight: "600",
      color: colors.text.muted,
    },
    pillActive: {
      backgroundColor: colors.accent.primary,
      color: colors.text.inverse,
    },
    sliderValue: {
      fontSize: 12,
      fontWeight: "600",
      color: colors.text.muted,
      marginTop: 4,
    },
    gameGroup: {
      marginTop: 12,
    },
    gameHeader: {
      marginHorizontal: 12,
      paddingHorizontal: 12,
      paddingVertical: 10,
      borderRadius: 12,
      backgroundColor: colors.surface.card,
      borderWidth: 1,
      borderColor: colors.border.subtle,
      flexDirection: "row",
      justifyContent: "space-between",
      alignItems: "center",
    },
    gameHeaderLeft: {
      flex: 1,
    },
    gameMatchupRow: {
      flexDirection: "row",
      alignItems: "center",
    },
    matchupLogoRow: {
      flexDirection: "row",
      alignItems: "center",
      marginRight: 8,
    },
    gameTeamLogo: {
      width: 20,
      height: 20,
      resizeMode: "contain",
    },
    gameAtSymbol: {
      marginHorizontal: 4,
      fontSize: 12,
      fontWeight: "700",
      color: colors.text.muted,
    },
    gameTitle: {
      fontSize: 14,
      fontWeight: "700",
      color: colors.text.primary,
      flexShrink: 1,
    },
    gameMeta: {
      marginTop: 2,
      fontSize: 12,
      fontWeight: "600",
      color: colors.text.muted,
    },
    gameChevron: {
      fontSize: 12,
      fontWeight: "700",
      color: colors.text.muted,
    },
    center: {
      flex: 1,
      justifyContent: "center",
      alignItems: "center",
    },
    loading: {
      fontSize: 14,
      fontWeight: "600",
      color: colors.text.muted,
    },
  });
