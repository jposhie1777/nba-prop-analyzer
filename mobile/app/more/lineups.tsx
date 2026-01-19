// mobile/app/%28tabs%29/lineups.tsx
import { ScrollView, Text } from "react-native";
import { useTheme } from "@/store/useTheme";
import { useTonightLineups } from "@/lib/api/useTonightLineups";
import { GameLineupCard } from "@/components/lineups/GameLineupCard";

export default function LineupsScreen() {
  const { colors } = useTheme();
  const { data, loading } = useTonightLineups();

  if (loading) {
    return (
      <Text style={{ color: colors.text.muted, padding: 16 }}>
        Loading lineups…
      </Text>
    );
  }

  if (!data.length) {
    return (
      <Text style={{ color: colors.text.muted, padding: 16 }}>
        No games tonight
      </Text>
    );
  }

  return (
    <ScrollView
      style={{ backgroundColor: colors.surface.screen }}
      contentContainerStyle={{ padding: 12, gap: 12 }}
    >
      {data.map(game => (
        <GameLineupCard key={game.game_id} game={game} />
      ))}
    </ScrollView>
  );
}