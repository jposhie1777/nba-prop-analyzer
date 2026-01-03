import { View, ScrollView } from "react-native";
import PropCard from "./components/PropCard";
import colors from "./theme/colors";
import { mockProps } from "./data/mockProps";

export default function App() {
  return (
    <View style={{ flex: 1, backgroundColor: colors.bg }}>
      <ScrollView showsVerticalScrollIndicator={false}>
        {mockProps.map((p) => (
          <PropCard
            key={p.id}
            matchup={p.matchup}
            player={p.player}
            market={p.market}
            hitRate={p.hitRate}
            edge={p.edge}
            confidence={p.confidence}
          />
        ))}
      </ScrollView>
    </View>
  );
}