import { EplTableScreen } from "@/components/epl/EplTableScreen";

export default function LaLigaTeamMasterMetricsScreen() {
  return (
    <EplTableScreen
      endpoint="/laliga/team-master-metrics"
      title="Team Master Metrics"
      subtitle="Complete team master table including season + rolling splits for goals and cards."
      leagueLabel="LaLiga"
      columns={[
        { key: "rank", label: "Rank" },
        { key: "standing_note", label: "Record" },
        { key: "points", label: "Points" },
        { key: "goal_differential", label: "Goal differential" },
        { key: "points_per_game", label: "Points per game" },
        { key: "season_avg_goals_scored", label: "Season avg goals scored" },
        { key: "season_avg_goals_allowed", label: "Season avg goals allowed" },
        { key: "season_score_rate", label: "Season scoring rate" },
        { key: "season_allow_rate", label: "Season allow rate" },
        { key: "last10_avg_scored", label: "Last 10 avg scored" },
        { key: "last10_avg_allowed", label: "Last 10 avg allowed" },
        { key: "last5_avg_scored", label: "Last 5 avg scored" },
        { key: "last5_avg_allowed", label: "Last 5 avg allowed" },
        { key: "last3_avg_scored", label: "Last 3 avg scored" },
        { key: "last3_avg_allowed", label: "Last 3 avg allowed" },
        { key: "season_team_cards_pg", label: "Season team cards / game" },
        { key: "season_opponent_cards_pg", label: "Season opp cards / game" },
        { key: "season_total_cards_pg", label: "Season total cards / game" },
        { key: "l10_team_cards_pg", label: "L10 team cards / game" },
        { key: "l10_opponent_cards_pg", label: "L10 opp cards / game" },
        { key: "l10_total_cards_pg", label: "L10 total cards / game" },
        { key: "l5_team_cards_pg", label: "L5 team cards / game" },
        { key: "l5_opponent_cards_pg", label: "L5 opp cards / game" },
        { key: "l5_total_cards_pg", label: "L5 total cards / game" },
        { key: "l3_team_cards_pg", label: "L3 team cards / game" },
        { key: "l3_opponent_cards_pg", label: "L3 opp cards / game" },
        { key: "l3_total_cards_pg", label: "L3 total cards / game" },
      ]}
    />
  );
}
