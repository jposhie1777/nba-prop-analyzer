// /screens/liveGamesScreen.tsx
import { FlatList, View, Text } from "react-native";

import { useTheme } from "@/store/useTheme";
import { LiveGameCard } from "@/components/live/LiveGameCard";
import { useLiveGames } from "@/hooks/useLiveGames";
import { useLivePlayerStats } from "@/hooks/useLivePlayerStats";

export default function LiveGamesScreen() {
  const { colors } = useTheme();

  const { games, mode } = useLiveGames();
  const { playersByGame } = useLivePlayerStats();

  const isConnecting = mode === "sse" && games.length === 0;

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

      {/* LIVE GAMES */}
      <FlatList
        data={games}
        keyExtractor={(g) => g.gameId}
        contentContainerStyle={{ paddingBottom: 16 }}
        renderItem={({ item }) => (
          <LiveGameCard
            game={item}
            players={playersByGame(item.game_id)}
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
    </View>
  );
}