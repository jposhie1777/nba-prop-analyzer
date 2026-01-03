import { View, ScrollView } from "react-native";
import PropCard from "../../components/PropCard";
import colors from "../../theme/color";

export default function HomeScreen() {
  return (
    <View style={{ flex: 1, backgroundColor: colors.bg }}>
      <ScrollView showsVerticalScrollIndicator={false}>
        <PropCard
          home="CHA"
          away="MIL"
          player="Miles Bridges"
          market="Points"
          line={18.5}
          odds={-110}
          hitRate={0.82}
          edge={0.11}
          confidence={78}
        />

        <PropCard
          home="CHA"
          away="MIL"
          player="Miles Bridges"
          market="Points"
          line={18.5}
          odds={-110}
          hitRate={0.82}
          edge={0.11}
          confidence={78}
        />

        <PropCard
          home="CHA"
          away="MIL"
          player="Miles Bridges"
          market="Points"
          line={18.5}
          odds={-110}
          hitRate={0.82}
          edge={0.11}
          confidence={78}
        />
      </ScrollView>
    </View>
  );
}
