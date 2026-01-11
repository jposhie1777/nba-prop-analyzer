// components/live/GameHeader
import { View, Text, Image, StyleSheet } from "react-native";
import { useTheme } from "@/store/useTheme";
import { TeamSide } from "@/types/live";
import { TEAM_LOGOS } from "@/utils/teamLogos";

type Props = {
  home: TeamSide;
  away: TeamSide;
};

export function GameHeader({ home, away }: Props) {
  return (
    <View style={styles.row}>
      <Team team={away} />
      <Text style={styles.at}>@</Text>
      <Team team={home} />
    </View>
  );
}

function Team({ team }: { team: TeamSide }) {
  const { colors } = useTheme();

  const logoUri =
    TEAM_LOGOS[team.abbreviation] ??
    "https://a.espncdn.com/i/teamlogos/nba/500/nba.png";

  return (
    <View style={styles.team}>
      <Image source={{ uri: logoUri }} style={styles.logo} />
      <Text style={[styles.text, { color: colors.text.primary }]}>
        {team.abbreviation}
      </Text>
    </View>
  );
}

const styles = {
  row: {
    flexDirection: "row",
    justifyContent: "space-between",
    alignItems: "center",
    marginBottom: 8, // 👈 increase
  },
  team: {
    flexDirection: "row",
    alignItems: "center",
    minWidth: 72,
  },
  logo: {
    width: 28,
    height: 28,
    marginRight: 6,
    resizeMode: "contain",
    opacity: 0.95,
  },
  text: {
    fontSize: 14,
    fontWeight: "600",
  },
  at: {
    fontSize: 12,
    opacity: 0.45,
  },
} as const;