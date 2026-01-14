import { View, Text } from "react-native";
import { useTheme } from "@/store/useTheme";
import { PlayerPropCard } from "./PlayerPropCard";

type Props = {
  groupedProps: any[]; // already filtered + sorted
  loading: boolean;
  playerNameById: Map<number, string>;
  playerMetaById: Map<number, any>;
};

export function LiveOdds({
  groupedProps,
  loading,
  playerNameById,
  playerMetaById,
}: Props) {
  const { colors } = useTheme();

  if (loading) {
    return (
      <Text style={{ color: colors.text.muted, marginTop: 8 }}>
        Loading live props…
      </Text>
    );
  }

  if (!groupedProps.length) {
    return (
      <Text style={{ color: colors.text.muted, marginTop: 8 }}>
        No live props available
      </Text>
    );
  }

  return (
    <View style={{ gap: 12, marginTop: 8 }}>
      {groupedProps.map((p) => {
        const meta = playerMetaById.get(p.player_id);
        if (!meta) return null;
  
        return (
          <PlayerPropCard
            key={`${p.player_id}-${p.market}`}
            player={p}
            name={playerNameById.get(p.player_id)}
            minutes={meta.minutes}
            current={meta}
          />
        );
      })}
    </View>
  );
}