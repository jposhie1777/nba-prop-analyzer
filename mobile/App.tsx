import "react-native-gesture-handler";

import { SafeAreaView, ScrollView } from "react-native";
import { GestureHandlerRootView } from "react-native-gesture-handler";

import PropCard from "./components/PropCard";
import DebugMemory from "./components/debug/DebugMemory";
import colors from "./theme/color";

export default function App() {
  console.log("__DEV__", __DEV__);

  return (
    <GestureHandlerRootView style={{ flex: 1 }}>
      <DebugMemory />

      <SafeAreaView style={{ flex: 1, backgroundColor: colors.bg }}>
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
            confidence={82}
          />
        </ScrollView>
      </SafeAreaView>
    </GestureHandlerRootView>
  );
}
