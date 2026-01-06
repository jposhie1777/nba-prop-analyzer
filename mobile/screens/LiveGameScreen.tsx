import { FlatList, View, ActivityIndicator, Text } from "react-native";

import { useTheme } from "@/store/useTheme";
import { LiveGameCard } from "@/components/live/LiveGameCard";
import { useLiveGames } from "@/hooks/useLiveGames";

export default function LiveGamesScreen() {
  const { colors } = useTheme();

  // 🔴 Live data now comes from the hybrid hook
  const { games, mode } = useLiveGames();

  // ✅ ADD THIS RIGHT HERE ⬇️
  console.log("🖥️ LiveGameScreen render", {
    mode,
    gameCount: games.length,
  });

  const loading = games.length === 0 && mode === "sse";

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
        <ActivityIndicator size="large" color={colors.brand.primary} />
      </View>
    );
  }

  /* =============================
     Empty
  ============================== */
  if (!games.length) {
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
          No live games right now
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

      <FlatList
        data={games}
        keyExtractor={(g) => g.gameId}
        contentContainerStyle={{ paddingBottom: 24 }}
        renderItem={({ item }) => <LiveGameCard game={item} />}
      />
    </View>
  );
}
