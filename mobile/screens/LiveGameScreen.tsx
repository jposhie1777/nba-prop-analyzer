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
  const { games, mode } = useLiveGames();
  const { games: scheduleGames } = useLiveGameSchedule();
  const { playersByGame } = useLivePlayerStats();

  const upcomingGames = scheduleGames.filter(
    (g) => g.state === "UPCOMING"
  );

  const liveGameIds = new Set(games.map((g) => g.gameId));

  const upcoming = upcomingGames.filter(
    (g) => !liveGameIds.has(g.game_id)
  );

  const isConnecting = mode === "sse" && games.length === 0;

  return (
    <View style={{ flex: 1, backgroundColor: colors.surface.screen }}>

      {/* Status bar (never replaces UI) */}
      <Text
        style={{
          color: colors.text.muted,
          fontSize: 12,
          paddingHorizontal: 12,
          paddingVertical: 6,
        }}
      >
        {mode === "sse"
          ? games.length
            ? "LIVE"
            : "Connecting live…"
          : "Refreshing"}
      </Text>

      {/* LIVE GAMES — ALWAYS SHOWN */}
      <FlatList
        data={games}
        keyExtractor={(g) => g.gameId}
        contentContainerStyle={{ paddingBottom: 16 }}
        renderItem={({ item }) => (
          <LiveGameCard
            game={item}
            players={playersByGame(Number(item.gameId))}
          />
        )}
      />

      {/* UPCOMING GAMES (below live) */}
      {upcoming.length > 0 && (
        <View style={{ paddingBottom: 24 }}>
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

      {/* EMPTY STATE — ONLY IF ABSOLUTELY NOTHING */}
      {games.length === 0 && upcoming.length === 0 && !isConnecting && (
        <View
          style={{
            padding: 24,
            alignItems: "center",
          }}
        >
          <Text style={{ color: colors.text.muted }}>
            No games right now
          </Text>
        </View>
      )}
    </View>
  );
}
