import { router } from "expo-router";

export function openTrendChart(
  player: string,
  market: string
) {
  router.push({
    pathname: "/trend",
    params: {
      player,
      market,
    },
  });
}