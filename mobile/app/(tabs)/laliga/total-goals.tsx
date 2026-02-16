import { EplTableScreen } from "@/components/epl/EplTableScreen";

export default function LaLigaTotalGoalsScreen() {
  return (
    <EplTableScreen
      endpoint="/laliga/total-goals"
      title="Total Goals"
      subtitle="Projected totals and supporting team-level scoring metrics."
      leagueLabel="LaLiga"
      columns={[
        { key: "projected_total_goals", label: "Projected total goals" },
        { key: "opponent_total_avg_goals", label: "Opponent total avg goals" },
        { key: "home_avg_goals", label: "Home avg goals" },
        { key: "home_avg_goals_allowed", label: "Home avg goals allowed" },
        { key: "away_avg_goals", label: "Away avg goals" },
        { key: "away_avg_goals_allowed", label: "Away avg goals allowed" },
        { key: "over_2_5_rate_blend", label: "Over 2.5 blended rate" },
        { key: "total_goals_volatility", label: "Total goals volatility" },
      ]}
    />
  );
}
