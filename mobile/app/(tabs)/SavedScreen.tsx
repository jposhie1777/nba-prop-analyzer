import { View, ScrollView, Text } from "react-native";
import PropCard from "../../components/PropCard";
import colors from "../../theme/color";
import { MOCK_PROPS } from "../../data/props";

type Props = {
  savedIds: Set<string>;
  onToggleSave: (id: string) => void;
};

export default function SavedScreen({ savedIds, onToggleSave }: Props) {
  const savedProps = MOCK_PROPS.filter((p) => savedIds.has(p.id));

  if (savedProps.length === 0) {
    return (
      <View style={{ flex: 1, backgroundColor: colors.bg, padding: 24 }}>
        <Text style={{ color: colors.textSecondary }}>
          No saved bets yet.
        </Text>
      </View>
    );
  }

  return (
    <View style={{ flex: 1, backgroundColor: colors.bg }}>
      <ScrollView>
        {savedProps.map((prop) => (
          <PropCard
            key={prop.id}
            {...prop}
            saved
            onToggleSave={() => onToggleSave(prop.id)}
          />
        ))}
      </ScrollView>
    </View>
  );
}