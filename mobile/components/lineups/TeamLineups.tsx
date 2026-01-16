import { View, Text } from "react-native";
import { useTheme } from "@/store/useTheme";
import { LineupRow } from "./LineupRow";

export function TeamLineups({
  teamAbbr,
  mostCommon,
  projected,
}: {
  teamAbbr: string;
  mostCommon: any[];
  projected: any[];
}) {
  const { colors } = useTheme();

  const mc = mostCommon.filter(l => l.team_abbr === teamAbbr);
  const proj = projected.filter(l => l.team_abbr === teamAbbr);

  return (
    <View style={{ gap: 8 }}>
      <Text style={{ color: colors.text.primary, fontWeight: "600" }}>
        {teamAbbr}
      </Text>

      {/* Most Common */}
      <View style={{ gap: 4 }}>
        <Text style={{ color: colors.text.muted, fontSize: 12 }}>
          Most Common Starters
        </Text>
        {mc.map(p => (
          <LineupRow key={`mc-${p.player_id}`} player={p} />
        ))}
      </View>

      {/* Projected */}
      <View style={{ gap: 4 }}>
        <Text style={{ color: colors.text.muted, fontSize: 12 }}>
          Projected Starters
        </Text>
        {proj.map(p => (
          <LineupRow key={`proj-${p.player_id}`} player={p} />
        ))}
      </View>
    </View>
  );
}