import { EplTableScreen } from "@/components/epl/EplTableScreen";

export default function MlsBttsScreen() {
  return (
    <EplTableScreen
      endpoint="/mls/btts"
      title="Both Teams To Score"
      subtitle="BTTS model using each side's historical scoring and concession rates."
      leagueLabel="MLS"
      columns={[
        { key: "btts_yes_pct_model", label: "BTTS Yes model %" },
        { key: "home_btts_hist", label: "Home BTTS historical" },
        { key: "away_btts_hist", label: "Away BTTS historical" },
      ]}
    />
  );
}
