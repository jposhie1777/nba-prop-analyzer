import { ScrollView, Text } from "react-native";
import { useFirstBasket } from "@/hooks/useFirstBasket";
import { groupFirstBasketByGame } from "@/utils/groupFirstBasketByGame";
import { FirstBasketGameCard } from "@/components/first-basket/FirstBasketGameCard";
import { useTheme } from "@/store/useTheme";

export default function FirstBasketScreen() {
  const { colors } = useTheme();
  const { data, isLoading } = useFirstBasket();

  if (isLoading) {
    return <Text style={{ color: colors.text.muted }}>Loading…</Text>;
  }

  const games = groupFirstBasketByGame(data ?? []);

  return (
    <ScrollView>
      {games.map(game => (
        <FirstBasketGameCard
          key={game.game_id}
          game={game}
        />
      ))}
    </ScrollView>
  );
}