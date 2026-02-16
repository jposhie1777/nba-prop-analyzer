import { EplTableScreen } from "@/components/epl/EplTableScreen";

export default function LaLigaMoneylinesScreen() {
  return (
    <EplTableScreen
      endpoint="/laliga/moneylines"
      title="Moneylines"
      subtitle="Model fair prices from team strength and scoring profiles."
      leagueLabel="LaLiga"
      columns={[
        { key: "home_win_pct_model", label: "Home win %" },
        { key: "away_win_pct_model", label: "Away win %" },
        { key: "home_fair_decimal", label: "Home fair decimal" },
        { key: "away_fair_decimal", label: "Away fair decimal" },
        { key: "home_goals_for_pg", label: "Home goals for / match" },
        { key: "home_goals_against_pg", label: "Home goals allowed / match" },
        { key: "away_goals_for_pg", label: "Away goals for / match" },
        { key: "away_goals_against_pg", label: "Away goals allowed / match" },
      ]}
    />
  );
}
