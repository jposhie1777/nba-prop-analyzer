import { EplTableScreen } from "@/components/epl/EplTableScreen";

export default function LaLigaStandingsScreen() {
  return (
    <EplTableScreen
      endpoint="/laliga/standings"
      title="Standings"
      subtitle="Current table with rank, W-L-D record, points, note, and goal differential."
      leagueLabel="LaLiga"
      columns={[
        { key: "rank", label: "Rank" },
        { key: "win_loss_record", label: "Record" },
        { key: "points", label: "Points" },
        { key: "standing_note", label: "Note" },
        { key: "goal_differential", label: "Goal differential" },
      ]}
    />
  );
}
