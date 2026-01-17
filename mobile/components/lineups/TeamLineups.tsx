// mobile/components/lineups/TeamLineups.tsx
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

  const projectedTeam = projected
    .filter(p => p.team_abbr === teamAbbr)
    .sort((a, b) => a.lineup_position - b.lineup_position);

  const mostCommonTeam = mostCommon
    .filter(m => m.team_abbr === teamAbbr)
    .sort((a, b) => a.lineup_position - b.lineup_position);

  // Track projected players so we don’t duplicate
  const usedPlayerIds = new Set(projectedTeam.map(p => p.player_id));

  // Fill missing projected slots from most common
  const filledProjected = [
    ...projectedTeam,
    ...mostCommonTeam.filter(m => !usedPlayerIds.has(m.player_id)),
  ].slice(0, 5);

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
        {mostCommonTeam.map(p => (
          <LineupRow
            key={`mc-${teamAbbr}-${p.lineup_position}-${p.player_id}`}
            player={p}
          />
        ))}
      </View>

      {/* Projected (with fallback) */}
      <View style={{ gap: 4 }}>
        <Text style={{ color: colors.text.muted, fontSize: 12 }}>
          Projected Starters
        </Text>
        {filledProjected.map((p, idx) => {
          const isFallback = !projectedTeam.some(
            x => x.player_id === p.player_id
          );

          return (
            <LineupRow
              key={`proj-${teamAbbr}-${idx}-${p.player_id}`}
              player={p}
              isFallback={isFallback}
            />
          );
        })}
      </View>
    </View>
  );
}
