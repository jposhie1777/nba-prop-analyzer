import { SafeAreaView, ScrollView } from "react-native";
import PropCard from "./components/PropCard";

export default function App() {
  return (
    <SafeAreaView style={{ flex: 1, backgroundColor: "#0B0F1A" }}>
      <ScrollView contentInsetAdjustmentBehavior="automatic">
        <PropCard
          player="Jayson Tatum"
          market="Points"
          line={28.5}
          odds={-110}
          hitRate={0.78}
          edge={0.12}
          home="BOS"
          away="NYK"
        />
      </ScrollView>
    </SafeAreaView>
  );
}
