// app/(tabs)/pga/index.tsx
import { ScrollView, View, Text, StyleSheet, Pressable, Platform } from "react-native";
import { useRouter } from "expo-router";
import { Ionicons } from "@expo/vector-icons";
import { useTheme } from "@/store/useTheme";

type IoniconName = keyof typeof Ionicons.glyphMap;

type TileProps = {
  title: string;
  subtitle: string;
  route: string;
  icon: IoniconName;
  iconBg: string;
  iconColor: string;
};

function Tile({ title, subtitle, route, icon, iconBg, iconColor }: TileProps) {
  const router = useRouter();
  const { colors } = useTheme();

  return (
    <Pressable
      onPress={() => router.push(route)}
      style={({ pressed }) => [
        styles.tile,
        {
          backgroundColor: colors.surface.card,
          borderColor: colors.border.subtle,
          ...Platform.select({
            ios: {
              shadowColor: "#000",
              shadowOffset: { width: 0, height: 2 },
              shadowOpacity: 0.06,
              shadowRadius: 8,
            },
            android: { elevation: 2 },
            default: {},
          }),
          opacity: pressed ? 0.85 : 1,
          transform: [{ scale: pressed ? 0.98 : 1 }],
        },
      ]}
    >
      <View style={[styles.iconWrap, { backgroundColor: iconBg }]}>
        <Ionicons name={icon} size={20} color={iconColor} />
      </View>
      <View style={styles.tileContent}>
        <Text style={[styles.tileTitle, { color: colors.text.primary }]}>
          {title}
        </Text>
        <Text style={[styles.tileSub, { color: colors.text.muted }]}>
          {subtitle}
        </Text>
      </View>
      <Ionicons
        name="chevron-forward"
        size={18}
        color={colors.text.disabled ?? colors.text.muted}
        style={styles.chevron}
      />
    </Pressable>
  );
}

type SectionHeaderProps = {
  title: string;
  color: string;
};

function SectionHeader({ title, color }: SectionHeaderProps) {
  return (
    <View style={styles.sectionHeader}>
      <View style={[styles.sectionDot, { backgroundColor: color }]} />
      <Text style={[styles.sectionTitle, { color }]}>{title}</Text>
    </View>
  );
}

const TILE_COLORS = {
  green: { bg: "rgba(22,163,74,0.10)", fg: "#16A34A" },
  blue: { bg: "rgba(37,99,235,0.10)", fg: "#2563EB" },
  purple: { bg: "rgba(124,58,237,0.10)", fg: "#7C3AED" },
  orange: { bg: "rgba(234,88,12,0.10)", fg: "#EA580C" },
  teal: { bg: "rgba(13,148,136,0.10)", fg: "#0D9488" },
  rose: { bg: "rgba(225,29,72,0.10)", fg: "#E11D48" },
  amber: { bg: "rgba(217,119,6,0.10)", fg: "#D97706" },
  indigo: { bg: "rgba(79,70,229,0.10)", fg: "#4F46E5" },
  cyan: { bg: "rgba(6,182,212,0.10)", fg: "#06B6D4" },
  slate: { bg: "rgba(71,85,105,0.10)", fg: "#475569" },
  emerald: { bg: "rgba(5,150,105,0.10)", fg: "#059669" },
  sky: { bg: "rgba(2,132,199,0.10)", fg: "#0284C7" },
};

export default function PgaHome() {
  const { colors } = useTheme();

  return (
    <ScrollView
      style={{ flex: 1, backgroundColor: colors.surface.screen }}
      contentContainerStyle={styles.container}
    >
      {/* Hero Section */}
      <View style={[styles.hero, { backgroundColor: colors.accent.primary }]}>
        <View style={styles.heroIconRow}>
          <Ionicons name="golf-outline" size={28} color="#FFFFFF" />
        </View>
        <Text style={styles.heroTitle}>PGA Analytics</Text>
        <Text style={styles.heroSub}>
          Tournament results, course stats, and player profiles
        </Text>
      </View>

      {/* Player Analysis */}
      <SectionHeader title="PLAYER ANALYSIS" color={TILE_COLORS.green.fg} />
      <Tile
        title="Player Form + Consistency"
        subtitle="Recent form, volatility, and trend scores"
        route="/(tabs)/pga/player-form"
        icon="trending-up"
        iconBg={TILE_COLORS.green.bg}
        iconColor={TILE_COLORS.green.fg}
      />
      <Tile
        title="Matchup Ratings"
        subtitle="Head-to-head performance vs another player"
        route="/(tabs)/pga/matchups"
        icon="people"
        iconBg={TILE_COLORS.purple.bg}
        iconColor={TILE_COLORS.purple.fg}
      />
      <Tile
        title="Pairings Compare"
        subtitle="2-3 player comparison with best-bet pick"
        route="/(tabs)/pga/compare"
        icon="swap-horizontal"
        iconBg={TILE_COLORS.blue.bg}
        iconColor={TILE_COLORS.blue.fg}
      />

      {/* Course Intelligence */}
      <SectionHeader title="COURSE INTELLIGENCE" color={TILE_COLORS.teal.fg} />
      <Tile
        title="Course Fit Model"
        subtitle="Course history + comp courses"
        route="/(tabs)/pga/course-fit"
        icon="fitness"
        iconBg={TILE_COLORS.teal.bg}
        iconColor={TILE_COLORS.teal.fg}
      />
      <Tile
        title="Course Profile"
        subtitle="Hole distribution, yardage, par makeup"
        route="/(tabs)/pga/course-profile"
        icon="map"
        iconBg={TILE_COLORS.cyan.bg}
        iconColor={TILE_COLORS.cyan.fg}
      />
      <Tile
        title="Comp-Course Clusters"
        subtitle="Most similar courses to the target"
        route="/(tabs)/pga/course-comps"
        icon="git-compare"
        iconBg={TILE_COLORS.indigo.bg}
        iconColor={TILE_COLORS.indigo.fg}
      />

      {/* Tournament Insights */}
      <SectionHeader title="TOURNAMENT INSIGHTS" color={TILE_COLORS.orange.fg} />
      <Tile
        title="Tournament Difficulty"
        subtitle="Scoring environment and difficulty ranks"
        route="/(tabs)/pga/tournament-difficulty"
        icon="shield"
        iconBg={TILE_COLORS.orange.bg}
        iconColor={TILE_COLORS.orange.fg}
      />
      <Tile
        title="Region / Time Splits"
        subtitle="Player results by month and country"
        route="/(tabs)/pga/region-splits"
        icon="globe"
        iconBg={TILE_COLORS.amber.bg}
        iconColor={TILE_COLORS.amber.fg}
      />
      <Tile
        title="Cut Rates"
        subtitle="Make-cut vs missed-cut profiles"
        route="/(tabs)/pga/cut-rates"
        icon="cut"
        iconBg={TILE_COLORS.rose.bg}
        iconColor={TILE_COLORS.rose.fg}
      />

      {/* Probabilities & Simulations */}
      <SectionHeader title="SIMULATIONS" color={TILE_COLORS.indigo.fg} />
      <Tile
        title="Placement Probabilities"
        subtitle="Win/top-5/top-10/top-20 rates"
        route="/(tabs)/pga/placement-probabilities"
        icon="podium"
        iconBg={TILE_COLORS.emerald.bg}
        iconColor={TILE_COLORS.emerald.fg}
      />
      <Tile
        title="Simulated Finishes"
        subtitle="Monte Carlo finish distribution"
        route="/(tabs)/pga/simulated-finishes"
        icon="dice"
        iconBg={TILE_COLORS.sky.bg}
        iconColor={TILE_COLORS.sky.fg}
      />
      <Tile
        title="Simulated Leaderboard"
        subtitle="Tournament-wide projected finish rankings"
        route="/(tabs)/pga/simulated-leaderboard"
        icon="trophy"
        iconBg={TILE_COLORS.amber.bg}
        iconColor={TILE_COLORS.amber.fg}
      />
    </ScrollView>
  );
}

const styles = StyleSheet.create({
  container: {
    padding: 16,
    paddingBottom: 48,
  },
  hero: {
    borderRadius: 16,
    padding: 20,
    marginBottom: 20,
  },
  heroIconRow: {
    marginBottom: 10,
  },
  heroTitle: {
    fontSize: 24,
    fontWeight: "800",
    color: "#FFFFFF",
    letterSpacing: -0.3,
  },
  heroSub: {
    fontSize: 13,
    color: "rgba(255,255,255,0.75)",
    marginTop: 4,
    lineHeight: 18,
  },
  sectionHeader: {
    flexDirection: "row",
    alignItems: "center",
    marginTop: 20,
    marginBottom: 8,
    paddingLeft: 2,
  },
  sectionDot: {
    width: 8,
    height: 8,
    borderRadius: 4,
    marginRight: 8,
  },
  sectionTitle: {
    fontSize: 11,
    fontWeight: "700",
    letterSpacing: 1.2,
  },
  tile: {
    flexDirection: "row",
    alignItems: "center",
    borderWidth: StyleSheet.hairlineWidth,
    borderRadius: 14,
    padding: 14,
    marginBottom: 8,
  },
  iconWrap: {
    width: 40,
    height: 40,
    borderRadius: 12,
    alignItems: "center",
    justifyContent: "center",
    marginRight: 12,
  },
  tileContent: {
    flex: 1,
  },
  tileTitle: {
    fontSize: 15,
    fontWeight: "700",
    letterSpacing: -0.1,
  },
  tileSub: {
    marginTop: 2,
    fontSize: 12,
    lineHeight: 16,
  },
  chevron: {
    marginLeft: 8,
  },
});
