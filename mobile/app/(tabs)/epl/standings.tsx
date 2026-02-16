import { EplTableScreen } from "@/components/epl/EplTableScreen";

export default function EplStandingsScreen() {
  return (
    <EplTableScreen
      endpoint="/epl/standings"
      title="Standings"
      subtitle="Current table with rank, points, record, and goal differential."
      columns={[
        { key: "rank", label: "Rank" },
        { key: "win_loss_record", label: "Record" },
        { key: "points", label: "Points" },
        { key: "goal_differential", label: "Goal differential" },
      ]}
    />
  );
}
