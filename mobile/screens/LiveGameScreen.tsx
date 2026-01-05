import { FlatList, View, ActivityIndicator, Text } from "react-native";
import { useEffect, useRef, useState } from "react";

import { useTheme } from "@/store/useTheme";
import { LiveGameCard } from "@/components/live/LiveGameCard";
import { LiveGame } from "@/types/live";
import { fetchLiveGames } from "@/services/liveGames";

const POLL_INTERVAL_MS = 20_000; // 20 seconds

export default function LiveGamesScreen() {
  const { colors } = useTheme();

  const [games, setGames] = useState<LiveGame[]>([]);
  const [loading, setLoading] = useState(true);
  const pollRef = useRef<NodeJS.Timeout | null>(null);

  useEffect(() => {
    let mounted = true;

    const load = async () => {
      const data = await fetchLiveGames();
      if (!mounted) return;

      setGames(data);
      setLoading(false);
    };

    // Initial fetch immediately
    load();

    // Poll every 20 seconds
    pollRef.current = setInterval(load, POLL_INTERVAL_MS);

    // Cleanup
    return () => {
      mounted = false;
      if (pollRef.current) {
        clearInterval(pollRef.current);
      }
    };
  }, []);

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
      <FlatList
        data={games}
        keyExtractor={(g) => g.gameId}
        contentContainerStyle={{ paddingBottom: 24 }}
        renderItem={({ item }) => <LiveGameCard game={item} />}
      />
    </View>
  );
}
