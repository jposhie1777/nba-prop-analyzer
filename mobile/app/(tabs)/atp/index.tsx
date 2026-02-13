import { ScrollView, Text, StyleSheet, Pressable, View } from "react-native";
import { useRouter } from "expo-router";
import { Ionicons } from "@expo/vector-icons";

import { useTheme } from "@/store/useTheme";
import ThemeSelectorSection from "@/components/ThemeSelectorSection";

type TileProps = {
  title: string;
  subtitle: string;
  route: string;
  icon: keyof typeof Ionicons.glyphMap;
  accent: string;
};

function TopStat({ label, value }: { label: string; value: string }) {
  const { colors } = useTheme();

  return (
    <View
      style={[
        styles.topStat,
        {
          backgroundColor: colors.surface.card,
          borderColor: colors.border.subtle,
        },
      ]}
    >
      <Text style={[styles.topStatLabel, { color: colors.text.muted }]}>{label}</Text>
      <Text style={[styles.topStatValue, { color: colors.text.primary }]}>{value}</Text>
    </View>
  );
}

function Tile({ title, subtitle, route, icon, accent }: TileProps) {
  const router = useRouter();
  const { colors } = useTheme();

  return (
    <Pressable
      onPress={() => router.push(route)}
      style={[
        styles.tile,
        {
          backgroundColor: "#0B1529",
          borderColor: colors.border.subtle,
        },
      ]}
    >
      <View style={[styles.tileGlow, { backgroundColor: `${accent}16` }]} />
      <View style={styles.tileHeader}>
        <View style={styles.tileTitleRow}>
          <View style={[styles.tileIconBubble, { backgroundColor: `${accent}25` }]}>
            <Ionicons name={icon} size={15} color={accent} />
          </View>
          <Text style={[styles.tileTitle, { color: colors.text.primary }]}>{title}</Text>
        </View>
        <Ionicons name="chevron-forward" size={16} color={colors.text.muted} />
      </View>
      <Text style={[styles.tileSub, { color: colors.text.muted }]}>{subtitle}</Text>
    </Pressable>
  );
}

export default function AtpHome() {
  const router = useRouter();
  const { colors } = useTheme();

  return (
    <ScrollView style={styles.screen} contentContainerStyle={styles.content}>
      <View style={[styles.hero, { borderColor: colors.border.subtle }]}> 
        <View style={styles.heroGlow} />
        <View style={styles.heroTopRow}>
          <Text style={styles.eyebrow}>ATP DASHBOARD</Text>
          <View style={styles.livePill}>
            <View style={styles.liveDot} />
            <Text style={styles.liveText}>Live Draws</Text>
          </View>
        </View>

        <Text style={styles.h1}>ATP Historical + Betting Analytics</Text>
        <Text style={styles.sub}>
          Surface intelligence, player form, matchup trends, and live bracket workflows in one clean hub.
        </Text>

        <View style={styles.topStatsRow}>
          <TopStat label="Core Tools" value="8 Modules" />
          <TopStat label="Live Focus" value="Brackets" />
          <TopStat label="Primary Edge" value="Form + Surface" />
        </View>

        <Pressable
          onPress={() => router.push("/(tabs)/atp-bracket")}
          style={[styles.primaryButton, { backgroundColor: colors.accent.primary }]}
        >
          <Ionicons name="trophy-outline" size={15} color={colors.text.inverse} />
          <Text style={[styles.primaryButtonText, { color: colors.text.inverse }]}>View Live Tournament Bracket</Text>
        </Pressable>
      </View>

      <ThemeSelectorSection title="Theme selector" />

      <Text style={[styles.sectionLabel, { color: colors.text.muted }]}>ANALYSIS MODULES</Text>

      <Tile
        title="Player Form + Consistency"
        subtitle="Recent win rate, straight-sets, tiebreak trends"
        route="/(tabs)/atp/player-form"
        icon="pulse"
        accent="#39E4C9"
      />
      <Tile
        title="Tournament Bracket"
        subtitle="Live tournament draw, surface, and upcoming matches"
        route="/(tabs)/atp-bracket"
        icon="git-network"
        accent="#5BA8FF"
      />
      <Tile
        title="Surface Splits"
        subtitle="Win rate, straight-sets, and average sets by surface"
        route="/(tabs)/atp/surface-splits"
        icon="tennisball"
        accent="#F5C26B"
      />
      <Tile
        title="Tournament Performance"
        subtitle="Titles, finals, semis, and win rates"
        route="/(tabs)/atp/tournament-performance"
        icon="podium"
        accent="#8FA8FF"
      />
      <Tile
        title="Head-to-Head"
        subtitle="Series record with surface breakdowns"
        route="/(tabs)/atp/head-to-head"
        icon="people"
        accent="#4EE09D"
      />
      <Tile
        title="Matchup Compare"
        subtitle="Composite betting edge from form, surface, H2H, ranking"
        route="/(tabs)/atp/compare"
        icon="stats-chart"
        accent="#7DD3FC"
      />
      <Tile
        title="Region / Time Splits"
        subtitle="Win rates by month and location"
        route="/(tabs)/atp/region-splits"
        icon="earth"
        accent="#C4A5FF"
      />
      <Tile
        title="Set Distribution"
        subtitle="Set score outcomes for wins and losses"
        route="/(tabs)/atp/set-distribution"
        icon="layers"
        accent="#F39EA8"
      />
    </ScrollView>
  );
}

const styles = StyleSheet.create({
  screen: {
    flex: 1,
    backgroundColor: "#050A18",
  },
  content: {
    padding: 16,
    paddingBottom: 44,
  },
  hero: {
    borderWidth: StyleSheet.hairlineWidth,
    borderRadius: 18,
    padding: 16,
    backgroundColor: "#071731",
    overflow: "hidden",
    marginBottom: 12,
  },
  heroGlow: {
    position: "absolute",
    right: -70,
    top: -60,
    width: 220,
    height: 220,
    borderRadius: 999,
    backgroundColor: "rgba(91,168,255,0.18)",
  },
  heroTopRow: {
    flexDirection: "row",
    justifyContent: "space-between",
    alignItems: "center",
  },
  eyebrow: {
    color: "#90B3E9",
    fontSize: 11,
    fontWeight: "700",
    letterSpacing: 1,
  },
  livePill: {
    flexDirection: "row",
    alignItems: "center",
    gap: 6,
    backgroundColor: "rgba(57,228,201,0.14)",
    borderColor: "rgba(57,228,201,0.3)",
    borderWidth: 1,
    borderRadius: 999,
    paddingHorizontal: 10,
    paddingVertical: 5,
  },
  liveDot: {
    width: 6,
    height: 6,
    borderRadius: 999,
    backgroundColor: "#39E4C9",
  },
  liveText: {
    color: "#8BF4E0",
    fontSize: 11,
    fontWeight: "700",
  },
  h1: {
    marginTop: 14,
    fontSize: 28,
    fontWeight: "800",
    color: "#E9F2FF",
  },
  sub: {
    marginTop: 6,
    fontSize: 13,
    lineHeight: 19,
    color: "#A7C0E8",
    maxWidth: "95%",
  },
  topStatsRow: {
    marginTop: 14,
    flexDirection: "row",
    gap: 8,
  },
  topStat: {
    flex: 1,
    borderWidth: StyleSheet.hairlineWidth,
    borderRadius: 12,
    paddingVertical: 10,
    paddingHorizontal: 10,
  },
  topStatLabel: {
    fontSize: 11,
  },
  topStatValue: {
    marginTop: 4,
    fontSize: 13,
    fontWeight: "700",
  },
  primaryButton: {
    marginTop: 14,
    borderRadius: 12,
    paddingVertical: 12,
    paddingHorizontal: 14,
    alignItems: "center",
    justifyContent: "center",
    flexDirection: "row",
    gap: 8,
  },
  primaryButtonText: {
    fontSize: 15,
    fontWeight: "800",
  },
  sectionLabel: {
    marginTop: 4,
    marginBottom: 6,
    fontSize: 11,
    letterSpacing: 1,
    fontWeight: "700",
  },
  tile: {
    borderWidth: StyleSheet.hairlineWidth,
    borderRadius: 14,
    padding: 14,
    marginTop: 10,
    overflow: "hidden",
  },
  tileGlow: {
    position: "absolute",
    top: -46,
    right: -42,
    width: 120,
    height: 120,
    borderRadius: 999,
  },
  tileHeader: {
    flexDirection: "row",
    alignItems: "center",
    justifyContent: "space-between",
  },
  tileTitleRow: {
    flexDirection: "row",
    alignItems: "center",
    gap: 10,
    flex: 1,
    paddingRight: 8,
  },
  tileIconBubble: {
    width: 28,
    height: 28,
    borderRadius: 999,
    alignItems: "center",
    justifyContent: "center",
  },
  tileTitle: {
    fontSize: 18,
    fontWeight: "800",
    flexShrink: 1,
  },
  tileSub: {
    marginTop: 8,
    fontSize: 13,
    lineHeight: 18,
  },
});
