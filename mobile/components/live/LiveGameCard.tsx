// components/live/liveGameCard
import { View, Text, StyleSheet, Pressable } from "react-native";
import { useTheme } from "@/store/useTheme";
import { LiveGame } from "@/types/live";
import { LivePlayerStat } from "@/hooks/useLivePlayerStats";

import { GameHeader } from "./GameHeader";
import { ScoreRow } from "./ScoreRow";
import { GameStatus } from "./GameStatus";
import { BoxScore } from "./boxscore/BoxScore";
import { LiveOdds } from "./LiveOdds";
import { useLivePlayerProps } from "@/hooks/useLivePlayerProps";
import { groupLiveProps } from "@/utils/groupLiveProps";
import { useMemo, useState } from "react";
import { useLiveGameOdds } from "@/hooks/useLiveGameOdds";
import { OddsButton } from "./OddsButton";

type Props = {
  game: LiveGame;
  players: LivePlayerStat[];
};

export function LiveGameCard({ game, players }: Props) {
  const { colors } = useTheme();

  const {
    props: liveProps,
    loading: oddsLoading,
  } = useLivePlayerProps(game.gameId);
  const [showPlayerProps, setShowPlayerProps] = useState(false);

  const groupedLiveProps = useMemo(
    () => groupLiveProps(liveProps),
    [liveProps]
  );
  
  const playerMetaById = useMemo(() => {
    const map = new Map<number, {
      minutes: number;
      pts: number;
      reb: number;
      ast: number;
      stl: number;
      blk: number;
      tov: number;
    }>();
  
    for (const p of players) {
      map.set(p.player_id, {
        minutes: p.minutes ?? 0,
        pts: p.pts ?? 0,
        reb: p.reb ?? 0,
        ast: p.ast ?? 0,
        stl: p.stl ?? 0,
        blk: p.blk ?? 0,
        tov: p.tov ?? 0,
      });
    }
  
    return map;
  }, [players]);
  
  const filteredGroupedProps = useMemo(() => {
    const out: Record<number, any> = {};
  
    for (const [playerIdStr, player] of Object.entries(groupedLiveProps)) {
      const playerId = Number(playerIdStr);
      const meta = playerMetaById.get(playerId);
  
      if (!meta) continue;
  
      const validMarkets: Record<string, any> = {};
  
      for (const [market, marketData] of Object.entries(player.markets)) {
        const normalizedMarket = market.toUpperCase();
      
        let current = 0;
        switch (normalizedMarket) {
          case "PTS":
          case "POINTS":
            current = meta.pts;
            break;
          case "REB":
          case "REBOUNDS":
            current = meta.reb;
            break;
          case "AST":
          case "ASSISTS":
            current = meta.ast;
            break;
          default:
            continue;
        }
      
        // 🔑 find main over/under line
        const mainLine = marketData.lines.find(
          (l: any) => l.line_type === "over_under"
        );
      
        if (!mainLine) continue;
      
        // ❌ remove dead lines
        if (current >= mainLine.line) continue;
      
        validMarkets[market] = marketData;
      }
  
      if (Object.keys(validMarkets).length > 0) {
        out[playerId] = {
          ...player,
          markets: validMarkets,
        };
      }
    }
  
    return out;
  }, [groupedLiveProps, playerMetaById]);
  
  const sortedGroupedProps = useMemo(() => {
    return Object.values(filteredGroupedProps).sort((a: any, b: any) => {
      const minA = playerMetaById.get(a.player_id)?.minutes ?? 0;
      const minB = playerMetaById.get(b.player_id)?.minutes ?? 0;
      return minB - minA;
    });
  }, [filteredGroupedProps, playerMetaById]);

  const {
    odds: gameOdds,
    loading: gameOddsLoading,
  } = useLiveGameOdds(game.gameId);
  
  console.log("🎯 GAME ODDS DEBUG", {
    gameId: game.gameId,
    count: gameOdds.length,
    sample: gameOdds[0],
  });
  
    // ------------------------------------
    // 🧠 Player ID → Name lookup (STEP 1)
    // ------------------------------------
    const playerNameById = useMemo(() => {
      const map = new Map<number, string>();
  
      players.forEach((p) => {
        map.set(p.player_id, p.name);
      });
  
      return map;
    }, [players]);
    console.log("🎯 LIVE PROPS PIPELINE DEBUG", {
      gameId: game.gameId,
      rawProps: liveProps.length,
      groupedPlayers: Object.keys(groupedLiveProps).length,
      filteredPlayers: Object.keys(filteredGroupedProps).length,
      sortedPlayers: sortedGroupedProps.length,
      sample: sortedGroupedProps[0],
    });

  const hasLiveOdds = sortedGroupedProps.length > 0;

  return (
    <View style={[styles.card, { backgroundColor: colors.surface.card }]}>
      <GameHeader home={game.home} away={game.away} />
      <ScoreRow game={game} />
      <GameStatus game={game} />
      
      <View style={[styles.divider, { backgroundColor: colors.border.subtle }]} />
      
      {/* 🔴 DEBUG 5 */}
      <Text
        style={{
          fontSize: 10,
          color: colors.text.muted,
          textAlign: "center",
          marginBottom: 4,
        }}
      >
        DEBUG players for game {game.game_id}: {players.length}
      </Text>
      
      <BoxScore
        homeTeam={game.home.abbrev}
        awayTeam={game.away.abbrev}
        players={players}
      />
      
      {/* -----------------------------
         🟢 LIVE GAME ODDS (ALWAYS VISIBLE)
      ----------------------------- */}
      {/* 🟢 LIVE GAME ODDS (BUTTONS) */}
      {gameOdds?.length > 0 && (
        <View style={{ marginBottom: 10, gap: 6 }}>
          {gameOdds.map((o) => (
            <View
              key={o.book}
              style={{ gap: 6 }}
            >
              <Text
                style={{
                  fontSize: 11,
                  color: colors.text.muted,
                  fontWeight: "700",
                }}
              >
                {o.book.toUpperCase()}
              </Text>
      
              <View style={{ flexDirection: "row", gap: 8 }}>
                {o.spread_away !== null && (
                  <OddsButton
                    label={`${game.away.abbrev} ${o.spread_away}`}
                    odds={o.spread_away_odds}
                  />
                )}
      
                {o.spread_home !== null && (
                  <OddsButton
                    label={`${game.home.abbrev} ${o.spread_home}`}
                    odds={o.spread_home_odds}
                  />
                )}
      
                {o.total !== null && (
                  <>
                    <OddsButton label={`O ${o.total}`} odds={o.over} />
                    <OddsButton label={`U ${o.total}`} odds={o.under} />
                  </>
                )}
              </View>
            </View>
          ))}
        </View>
      )}
      
      <View
        style={[
          styles.divider,
          { backgroundColor: colors.border.subtle },
        ]}
      />
      
      {/* -----------------------------
         🔽 PLAYER PROPS (COLLAPSIBLE)
      ----------------------------- */}
      <Pressable
        onPress={() => setShowPlayerProps((v) => !v)}
        style={{ marginTop: 6 }}
      >
        <Text
          style={{
            color: colors.text.secondary,
            fontSize: 12,
            fontWeight: "600",
          }}
        >
          Player Props {showPlayerProps ? "▾" : "▸"}
        </Text>
      </Pressable>
      
      {showPlayerProps && (
        <LiveOdds
          groupedProps={sortedGroupedProps}
          loading={oddsLoading}
          playerNameById={playerNameById}
          playerMetaById={playerMetaById}
        />
      )}
    </View>
  );
}
const styles = StyleSheet.create({
  card: {
    borderRadius: 16,
    marginHorizontal: 12,
    marginTop: 12,
    padding: 12,
  },

  divider: {
    height: 1,
    marginVertical: 8,
  },
});