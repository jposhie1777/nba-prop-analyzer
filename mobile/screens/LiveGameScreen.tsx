// /screens/liveGamesScreen.tsx
import { FlatList, View, Text } from "react-native";

import { useTheme } from "@/store/useTheme";
import { LiveGameCard } from "@/components/live/LiveGameCard";
import { useLiveGames } from "@/hooks/useLiveGames";
import { useLivePlayerStats } from "@/hooks/useLivePlayerStats";
import { Pressable } from "react-native";
import { useSavedBets } from "@/store/useSavedBets";
import { useState } from "react";

export default function LiveGamesScreen() {
  const { colors } = useTheme();

  const { games, mode } = useLiveGames();
  const { playersByGame } = useLivePlayerStats();

  // 🟢 BETSLIP STATE
  const [betslipOpen, setBetslipOpen] = useState(false);
  const savedIds = useSavedBets((s) => s.savedIds);
  const bets = useSavedBets((s) => s.bets);

  const isConnecting = mode === "sse" && games.length === 0;
  
    /* ===========================
       DEV GUARD: players ↔ game
    =========================== */
    const guardedPlayersByGame = (gameId: string) => {
      const players = playersByGame(gameId);
  
      if (__DEV__) {
        if (players.length === 0) {
          console.warn("🟠 GUARD: no players for game", gameId);
        } else {
          const teams = new Set(players.map((p) => p.team));
          if (teams.size < 2) {
            console.warn("🔴 GUARD: players not split by team", {
              gameId,
              teams: Array.from(teams),
              samplePlayer: players[0],
            });
          }
        }
      }
  
      return players;
    };

  return (
    <View style={{ flex: 1, backgroundColor: colors.surface.screen }}>

      {/* Status row */}
      <Text
        style={{
          color: colors.text.muted,
          fontSize: 12,
          paddingHorizontal: 12,
          paddingVertical: 6,
        }}
      >
        {mode === "sse"
          ? games.length > 0
            ? "LIVE"
            : "Waiting for games to go live…"
          : "Refreshing"}
      </Text>
      <Text
        style={{
          color: colors.text.muted,
          fontSize: 10,
          paddingHorizontal: 12,
          paddingBottom: 6,
        }}
      >
        DEBUG player-stats mode: {mode}
      </Text>
      {/* LIVE GAMES */}
      <FlatList
        data={games}
        keyExtractor={(g) => g.gameId}
        contentContainerStyle={{ paddingBottom: 120 }}
        renderItem={({ item }) => (
          <LiveGameCard
            game={item}
            players={guardedPlayersByGame(item.gameId)}
          />
        )}
      />
  
      {/* 🧾 BETSLIP BAR */}
      {savedIds.size > 0 && (
        <Pressable
          onPress={() => setBetslipOpen(true)}
          style={{
            position: "absolute",
            bottom: 16,
            left: 16,
            right: 16,
            backgroundColor: colors.accent.primary,
            borderRadius: 16,
            paddingVertical: 14,
            alignItems: "center",
          }}
        >
          <Text
            style={{
              color: colors.text.primary,
              fontWeight: "800",
              fontSize: 14,
            }}
          >
            View Betslip ({savedIds.size})
          </Text>
        </Pressable>
      )}

      {/* 🧾 BETSLIP DRAWER */}
      {betslipOpen && (
        <View
          style={{
            position: "absolute",
            left: 0,
            right: 0,
            bottom: 0,
            backgroundColor: colors.surface.card,
            borderTopLeftRadius: 20,
            borderTopRightRadius: 20,
            padding: 16,
            maxHeight: "70%",
          }}
        >
          <Text
            style={{
              fontWeight: "800",
              fontSize: 16,
              marginBottom: 12,
              color: colors.text.primary,
            }}
          >
            Betslip
          </Text>
      
          {Array.from(bets.values()).map((bet) => (
            <View
              key={bet.id}
              style={{ marginBottom: 10 }}
            >
              <Text style={{ fontWeight: "700" }}>
                {bet.player} · {bet.market}
              </Text>
          
              <Text style={{ fontSize: 12, color: colors.text.muted }}>
                {bet.side === "milestone"
                  ? `${bet.line}+`
                  : `${bet.side.toUpperCase()} ${bet.line}`}
                {bet.odds != null ? ` (${bet.odds})` : ""}
              </Text>
            </View>
          ))}
      
          <Pressable
            onPress={() => setBetslipOpen(false)}
            style={{
              marginTop: 16,
              alignItems: "center",
            }}
          >
            <Text
              style={{
                color: colors.accent.primary,
                fontWeight: "700",
              }}
            >
              Close
            </Text>
          </Pressable>
        </View>
      )}

      {/* EMPTY STATE */}
      {games.length === 0 && !isConnecting && (
        <View
          style={{
            padding: 24,
            alignItems: "center",
          }}
        >
          <Text style={{ color: colors.text.muted }}>
            No live games right now
          </Text>
        </View>
      )}
    </View>
  );
}