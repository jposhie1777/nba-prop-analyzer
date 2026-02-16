import { EplTableScreen } from "@/components/epl/EplTableScreen";

export default function LaLigaCardsScreen() {
  return (
    <EplTableScreen
      endpoint="/laliga/cards"
      title="Cards"
      subtitle="Team card profile for prop analysis (yellow/red/card points)."
      leagueLabel="LaLiga"
      columns={[
        { key: "matches_sample", label: "Matches sample" },
        { key: "avg_yellow_cards", label: "Avg yellow cards" },
        { key: "avg_red_cards", label: "Avg red cards" },
        { key: "avg_card_points", label: "Avg card points" },
        { key: "card_volatility", label: "Card volatility" },
      ]}
    />
  );
}
