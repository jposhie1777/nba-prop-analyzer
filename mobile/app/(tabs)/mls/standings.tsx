import { EplTableScreen } from "@/components/epl/EplTableScreen";

export default function MlsStandingsScreen() {
  return (
    <EplTableScreen
      endpoint="/mls/standings"
      title="Standings"
      subtitle="Current table with rank, points, record, and goal differential."
      leagueLabel="MLS"
      columns={[
        { key: "rank", label: "Rank" },
        { key: "win_loss_record", label: "Record" },
        { key: "points", label: "Points" },
        { key: "goal_differential", label: "Goal differential" },
      ]}
    />
  );
}
