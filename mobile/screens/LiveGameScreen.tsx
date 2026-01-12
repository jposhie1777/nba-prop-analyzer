// /screens/liveGamesScreen.tsx
import { FlatList, View, Text } from "react-native";

import { useTheme } from "@/store/useTheme";
import { LiveGameCard } from "@/components/live/LiveGameCard";
import { useLiveGames } from "@/hooks/useLiveGames";
import { useLivePlayerStats } from "@/hooks/useLivePlayerStats";
import { BetSlipBar } from "@/components/bets/BetSlipBar"; // ✅ ADD
import { useEffect } from "react";

useEffect(() => {
  console.log(
    "🧪 DISCORD WEBHOOK LOADED:",
    process.env.EXPO_PUBLIC_DISCORD_GAMBLY_WEBHOOK?.slice(0, 40)
  );

  // 🔴 TEMP TEST — REMOVE AFTER CONFIRMATION
  async function testDiscord() {
    try {
      const res = await fetch(
        process.env.EXPO_PUBLIC_DISCORD_GAMBLY_WEBHOOK!,
        {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({
            content: "🧪 Pulse test message (ignore)",
          }),
        }
      );
  
      const text = await res.text();
  
      console.log("DISCORD STATUS:", res.status);
      console.log("DISCORD RESPONSE:", text);
    } catch (err) {
      console.error("❌ Discord fetch threw error:", err);
    }
  }

  if (__DEV__) {
    testDiscord();
  }
}, []);


export default function LiveGamesScreen() {
  const { colors } = useTheme();

  const { games, mode } = useLiveGames();
  const { playersByGame } = useLivePlayerStats();

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
        contentContainerStyle={{ paddingBottom: 96 }} // 👈 IMPORTANT
        renderItem={({ item }) => (
          <LiveGameCard
            game={item}
            players={guardedPlayersByGame(item.gameId)}
          />
        )}
      />

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

      {/* 🧾 BET SLIP BAR (FLOATING) */}
      <BetSlipBar />
    </View>
  );
}