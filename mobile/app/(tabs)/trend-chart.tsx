import { TrendChartScreen } from "@/trend/TrendChartScreen";
import { useLocalSearchParams } from "expo-router";

export default function TrendTab() {
  const params = useLocalSearchParams<{
    player?: string;
    market?: string;
  }>();

  return (
    <TrendChartScreen
      initialPlayer={params.player}
      initialMarket={params.market}
    />
  );
}