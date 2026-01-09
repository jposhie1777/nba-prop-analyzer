// /screens/liveGamesScreen.tsx
import { FlatList, View, ActivityIndicator, Text } from "react-native";

import { useTheme } from "@/store/useTheme";
import { LiveGameCard } from "@/components/live/LiveGameCard";
import { useLiveGames } from "@/hooks/useLiveGames";
import { useLivePlayerStats } from "@/hooks/useLivePlayerStats";
import { useLiveGameSchedule } from "@/hooks/useLiveGameSchedule";
import { formatET } from "@/lib/time/formatET";

export default function LiveGamesScreen() {
  const { colors } = useTheme();

  // 🔴 Live data now comes from the hybrid hook
  const { games, mode } = useLiveGames();
  const { games: scheduleGames } = useLiveGameSchedule();

  // ✅ ADD THIS RIGHT HERE ⬇️
  console.log("🖥️ LiveGameScreen render", {
    mode,
    gameCount: games.length,
  });

  const loading = games.length === 0 && mode === "sse";
  const { playersByGame, players, mode: playerMode } = useLivePlayerStats();
  console.log("👥 Live players snapshot", {
    totalPlayers: players.length,
  });

  const upcomingGames = scheduleGames.filter(
    (g) => g.state === "UPCOMING"
  );
  const liveGameIds = new Set(games.map((g) => g.gameId));

  const upcoming = upcomingGames.filter(
    (g) => !liveGameIds.has(g.game_id)
  );

  /* =============================
     Loading
  ============================== */
  if (loading) {
    return (
      <View
        style={{
          flex: 1,
          backgroundColor: colors.surface.screen,
          alignItems: "center",
          justifyContent: "center",
        }}
      >
        <ActivityIndicator size="large" color={colors.accent.primary} />
      </View>
    );
  }

/* =============================
   Empty
============================== */
if (!games.length && !upcoming.length) {
  return (
    <View
      style={{
        flex: 1,
        backgroundColor: colors.surface.screen,
        alignItems: "center",
        justifyContent: "center",
      }}
    >
      <Text style={{ color: colors.text.muted }}>
        No games right now
      </Text>
    </View>
  );
}


  /* =============================
     List
  ============================== */
  return (
    <View style={{ flex: 1, backgroundColor: colors.surface.screen }}>
      {/* Optional debug / status */}
      <Text
        style={{
          color: colors.text.muted,
          fontSize: 12,
          paddingHorizontal: 12,
          paddingVertical: 6,
        }}
      >
        {mode === "sse" ? "LIVE" : "REFRESHING"}
      </Text>

      {/* 🆕 UPCOMING GAMES (NEW SECTION) */}
      {upcoming.length > 0 && (
        <View style={{ paddingBottom: 8 }}>
          {upcoming.map((g) => (
            <View
              key={g.game_id}
              style={{
                marginHorizontal: 12,
                marginTop: 8,
                padding: 12,
                borderRadius: 12,
                backgroundColor: colors.surface.cardSoft,
              }}
            >
              <Text style={{ color: colors.text.secondary }}>
                {g.away} @ {g.home}
              </Text>

              <Text
                style={{
                  color: colors.text.muted,
                  fontSize: 12,
                  marginTop: 4,
                }}
              >
                {formatET(g.start_time_et)}
              </Text>
            </View>
          ))}
        </View>
      )}

      {/* ✅ EXISTING LIVE LIST (UNCHANGED) */}
      <FlatList
        data={games}
        keyExtractor={(g) => g.gameId}
        contentContainerStyle={{ paddingBottom: 24 }}
        renderItem={({ item }) => (
          <LiveGameCard
            game={item}
            players={playersByGame(Number(item.gameId))}
          />
        )}
      />
    </View>
  );
}
