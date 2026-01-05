import { View, Text, Image, StyleSheet } from "react-native";
import { useTheme } from "@/store/useTheme";
import { TeamSide } from "@/types/live";

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

  return (
    <View style={styles.team}>
      <Image source={{ uri: team.logo }} style={styles.logo} />
      <Text style={[styles.text, { color: colors.text.primary }]}>
        {team.abbrev}
      </Text>
    </View>
  );
}

const styles = StyleSheet.create({
  row: {
    flexDirection: "row",
    justifyContent: "space-between",
    alignItems: "center",
  },
  team: {
    flexDirection: "row",
    alignItems: "center",
  },
  logo: {
    width: 28,
    height: 28,
    marginRight: 6,
    resizeMode: "contain",
  },
  text: {
    fontSize: 14,
    fontWeight: "600",
  },
  at: {
    fontSize: 12,
    opacity: 0.5,
  },
});
