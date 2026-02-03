// app/(tabs)/pga/index.tsx
import { ScrollView, Text, StyleSheet, Pressable } from "react-native";
import { useRouter } from "expo-router";
import { useTheme } from "@/store/useTheme";

type TileProps = {
  title: string;
  subtitle: string;
  route: string;
};

function Tile({ title, subtitle, route }: TileProps) {
  const router = useRouter();
  const { colors } = useTheme();

  return (
    <Pressable
      onPress={() => router.push(route)}
      style={[
        styles.tile,
        {
          backgroundColor: colors.surface.card,
          borderColor: colors.border.subtle,
        },
      ]}
    >
      <Text style={[styles.tileTitle, { color: colors.text.primary }]}>
        {title}
      </Text>
      <Text style={[styles.tileSub, { color: colors.text.muted }]}>
        {subtitle}
      </Text>
    </Pressable>
  );
}

export default function PgaHome() {
  const { colors } = useTheme();

  return (
    <ScrollView
      style={{ flex: 1, backgroundColor: colors.surface.screen }}
      contentContainerStyle={{ padding: 16, paddingBottom: 40 }}
    >
      <Text style={[styles.h1, { color: colors.text.primary }]}>
        PGA Analytics
      </Text>
      <Text style={[styles.sub, { color: colors.text.muted }]}>
        All-star tier analytics built from tournament results, course stats, and
        course profiles.
      </Text>

      <Tile
        title="1) Player Form + Consistency"
        subtitle="Recent form, volatility, and trend scores"
        route="/(tabs)/pga/player-form"
      />
      <Tile
        title="2) Course Fit Model"
        subtitle="Course history + comp courses"
        route="/(tabs)/pga/course-fit"
      />
      <Tile
        title="3) Tournament Difficulty"
        subtitle="Scoring environment and difficulty ranks"
        route="/(tabs)/pga/tournament-difficulty"
      />
      <Tile
        title="4) Matchup Ratings"
        subtitle="Head-to-head performance vs another player"
        route="/(tabs)/pga/matchups"
      />
      <Tile
        title="5) Placement Probabilities"
        subtitle="Win/top-5/top-10/top-20 rates"
        route="/(tabs)/pga/placement-probabilities"
      />
      <Tile
        title="6) Cut Rates"
        subtitle="Make-cut vs missed-cut profiles"
        route="/(tabs)/pga/cut-rates"
      />
      <Tile
        title="7) Course Profile"
        subtitle="Hole distribution, yardage, par makeup"
        route="/(tabs)/pga/course-profile"
      />
      <Tile
        title="8) Region / Time Splits"
        subtitle="Player results by month and country"
        route="/(tabs)/pga/region-splits"
      />
      <Tile
        title="9) Comp-Course Clusters"
        subtitle="Most similar courses to the target"
        route="/(tabs)/pga/course-comps"
      />
      <Tile
        title="10) Simulated Finishes"
        subtitle="Monte Carlo finish distribution"
        route="/(tabs)/pga/simulated-finishes"
      />
    </ScrollView>
  );
}

const styles = StyleSheet.create({
  h1: {
    fontSize: 20,
    fontWeight: "800",
    marginBottom: 6,
  },
  sub: {
    fontSize: 12,
    marginBottom: 14,
  },
  tile: {
    borderWidth: StyleSheet.hairlineWidth,
    borderRadius: 12,
    padding: 14,
    marginTop: 10,
  },
  tileTitle: {
    fontSize: 15,
    fontWeight: "800",
  },
  tileSub: {
    marginTop: 4,
    fontSize: 12,
  },
});
