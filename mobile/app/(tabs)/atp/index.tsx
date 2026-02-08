// app/(tabs)/atp/index.tsx
import { ScrollView, Text, StyleSheet, Pressable } from "react-native";
import { useRouter } from "expo-router";
import { useTheme } from "@/store/useTheme";
import ThemeSelectorSection from "@/components/ThemeSelectorSection";

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

export default function AtpHome() {
  const router = useRouter();
  const { colors } = useTheme();

  return (
    <ScrollView
      style={{ flex: 1, backgroundColor: colors.surface.screen }}
      contentContainerStyle={{ padding: 16, paddingBottom: 40 }}
    >
      <Text style={[styles.h1, { color: colors.text.primary }]}>
        ATP Historical + Betting Analytics
      </Text>
      <Text style={[styles.sub, { color: colors.text.muted }]}>
        Historical match results power surface splits, form tracking, head-to-head
        trends, and matchup previews built for tennis betting workflows.
      </Text>
      <Pressable
        onPress={() => router.push("/(tabs)/atp-bracket")}
        style={[
          styles.primaryButton,
          {
            backgroundColor: colors.accent.primary,
            borderColor: colors.border.subtle,
          },
        ]}
      >
        <Text style={[styles.primaryButtonText, { color: colors.text.inverse }]}>
          View Live Tournament Bracket
        </Text>
      </Pressable>
      <ThemeSelectorSection title="Theme selector" />

      <Tile
        title="1) Player Form + Consistency"
        subtitle="Recent win rate, straight-sets, tiebreak trends"
        route="/(tabs)/atp/player-form"
      />
      <Tile
        title="2) Tournament Bracket"
        subtitle="Live tournament draw, surface, and upcoming matches"
        route="/(tabs)/atp-bracket"
      />
      <Tile
        title="3) Surface Splits"
        subtitle="Win rate, straight-sets, and average sets by surface"
        route="/(tabs)/atp/surface-splits"
      />
      <Tile
        title="4) Tournament Performance"
        subtitle="Titles, finals, semis, and win rates"
        route="/(tabs)/atp/tournament-performance"
      />
      <Tile
        title="5) Head-to-Head"
        subtitle="Series record with surface breakdowns"
        route="/(tabs)/atp/head-to-head"
      />
      <Tile
        title="6) Matchup Compare"
        subtitle="Composite betting edge from form, surface, H2H, ranking"
        route="/(tabs)/atp/compare"
      />
      <Tile
        title="7) Region / Time Splits"
        subtitle="Win rates by month and location"
        route="/(tabs)/atp/region-splits"
      />
      <Tile
        title="8) Set Distribution"
        subtitle="Set score outcomes for wins and losses"
        route="/(tabs)/atp/set-distribution"
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
  primaryButton: {
    borderWidth: StyleSheet.hairlineWidth,
    borderRadius: 999,
    paddingVertical: 10,
    paddingHorizontal: 16,
    alignItems: "center",
    marginBottom: 6,
  },
  primaryButtonText: {
    fontSize: 13,
    fontWeight: "700",
  },
});
