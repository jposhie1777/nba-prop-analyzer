import { ScrollView, Text, StyleSheet, Pressable, View, Image } from "react-native";
import { useRouter } from "expo-router";
import { Ionicons } from "@expo/vector-icons";

import { useTheme } from "@/store/useTheme";
import ThemeSelectorSection from "@/components/ThemeSelectorSection";
import { LEAGUE_LOGOS } from "@/utils/leagueLogos";

type SportTileProps = {
  title: string;
  subtitle: string;
  onPress?: () => void;
  badge: string;
  accent: string;
  icon: keyof typeof Ionicons.glyphMap;
  logoUrl?: string;
  comingSoon?: boolean;
};

function StatChip({ label, value }: { label: string; value: string }) {
  const { colors } = useTheme();

  return (
    <View
      style={[
        styles.statChip,
        {
          backgroundColor: colors.surface.card,
          borderColor: colors.border.subtle,
        },
      ]}
    >
      <Text style={[styles.statLabel, { color: colors.text.muted }]}>{label}</Text>
      <Text style={[styles.statValue, { color: colors.text.primary }]}>{value}</Text>
    </View>
  );
}

function SportTile({
  title,
  subtitle,
  onPress,
  badge,
  accent,
  icon,
  logoUrl,
  comingSoon = false,
}: SportTileProps) {
  const { colors } = useTheme();

  return (
    <Pressable
      onPress={onPress}
      disabled={comingSoon}
      style={[
        styles.tile,
        {
          borderColor: colors.border.subtle,
          backgroundColor: "#0A1224",
        },
      ]}
    >
      <View style={[styles.tileGlow, { backgroundColor: `${accent}14` }]} />
      <View style={styles.tileHeader}>
        <View style={styles.titleRow}>
          <View style={[styles.iconBubble, { backgroundColor: `${accent}22` }]}>
            {logoUrl ? (
              <Image source={{ uri: logoUrl }} style={styles.leagueLogo} />
            ) : (
              <Ionicons name={icon} size={16} color={accent} />
            )}
          </View>
          <Text style={[styles.tileTitle, { color: colors.text.primary }]}>{title}</Text>
        </View>
        <View style={[styles.badge, { backgroundColor: `${accent}25` }]}>
          <Text style={[styles.badgeText, { color: accent }]}>{badge}</Text>
        </View>
      </View>

      <Text style={[styles.tileSub, { color: colors.text.muted }]}>{subtitle}</Text>

      <View style={[styles.tileFooter, { borderTopColor: colors.border.subtle }]}>
        <Text style={[styles.tileFooterText, { color: colors.text.muted }]}>
          {comingSoon ? "Coming soon" : "Open dashboard"}
        </Text>
        <Ionicons
          name={comingSoon ? "time-outline" : "arrow-forward"}
          size={14}
          color={colors.text.muted}
        />
      </View>
    </Pressable>
  );
}

export default function Home() {
  const router = useRouter();
  const { colors } = useTheme();

  return (
    <ScrollView style={{ flex: 1, backgroundColor: "#050A18" }} contentContainerStyle={styles.content}>
      <View style={[styles.hero, { borderColor: colors.border.subtle }]}> 
        <View style={styles.heroGlow} />
        <View style={styles.heroTopRow}>
          <Text style={styles.eyebrow}>PROP ANALYZER</Text>
          <View style={styles.livePill}>
            <View style={styles.liveDot} />
            <Text style={styles.livePillText}>Live Models</Text>
          </View>
        </View>

        <Text style={styles.h1}>Research Hub</Text>
        <Text style={styles.subtitle}>
          Build cards, compare edges, and jump into each sport&apos;s analytics stack.
        </Text>

        <View style={styles.statsRow}>
          <StatChip label="Sports" value="2 Active" />
          <StatChip label="Live Markets" value="20+" />
          <StatChip label="Tools" value="Models + EV" />
        </View>
      </View>

      <ThemeSelectorSection title="Theme" />


      <SportTile
        title="PGA"
        subtitle="Course fit, tournament sims, placements, and matchup tools."
        badge="NEW"
        icon="golf"
        logoUrl={LEAGUE_LOGOS.PGA}
        accent="#7DD3FC"
        onPress={() => router.push("/(tabs)/pga")}
      />

      <SportTile
        title="ATP"
        subtitle="Surface splits, player form, bracket projections, and H2H."
        badge="TRACKING"
        icon="tennisball"
        logoUrl={LEAGUE_LOGOS.ATP}
        accent="#F4C76C"
        onPress={() => router.push("/(tabs)/atp")}
      />

      <SportTile
        title="EPL"
        subtitle="Moneylines, BTTS, totals, and cards all tied into one hub."
        badge="LIVE"
        icon="football"
        logoUrl={LEAGUE_LOGOS.EPL}
        accent="#A78BFA"
        onPress={() => router.push("/(tabs)/epl")}
      />


      <SportTile
        title="LaLiga"
        subtitle="Moneylines, BTTS, totals, cards, and standings for Spain's top flight."
        badge="LIVE"
        icon="football"
        logoUrl={LEAGUE_LOGOS.LALIGA}
        accent="#F59E0B"
        onPress={() => router.push("/(tabs)/laliga")}
      />

      <SportTile
        title="MLS"
        subtitle="US league dashboards with the same analytics stack as EPL."
        badge="LIVE"
        icon="football"
        logoUrl={LEAGUE_LOGOS.MLS}
        accent="#34D399"
        onPress={() => router.push("/(tabs)/mls")}
      />

      <SportTile
        title="WTA"
        subtitle="Women's tennis dashboards and matchup models are coming soon."
        badge="COMING SOON"
        icon="tennisball"
        logoUrl={LEAGUE_LOGOS.WTA}
        accent="#FB7185"
        comingSoon
      />


      <View
        style={[
          styles.section,
          {
            borderColor: colors.border.subtle,
            backgroundColor: "#0A1224",
          },
        ]}
      >
        <Text style={[styles.sectionTitle, { color: colors.text.primary }]}>Need another league?</Text>
        <Text style={[styles.sectionSub, { color: colors.text.muted }]}>This hub is modular — new sports can be added quickly with the same polished experience.</Text>
      </View>
    </ScrollView>
  );
}

const styles = StyleSheet.create({
  content: {
    padding: 16,
    paddingBottom: 40,
    gap: 12,
  },
  hero: {
    borderWidth: StyleSheet.hairlineWidth,
    borderRadius: 18,
    padding: 16,
    marginBottom: 4,
    backgroundColor: "#071731",
    overflow: "hidden",
  },
  heroGlow: {
    position: "absolute",
    right: -80,
    top: -70,
    width: 220,
    height: 220,
    borderRadius: 999,
    backgroundColor: "rgba(57,228,201,0.14)",
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
  livePillText: {
    color: "#8BF4E0",
    fontSize: 11,
    fontWeight: "700",
  },
  h1: {
    marginTop: 16,
    fontSize: 28,
    fontWeight: "800",
    color: "#E9F2FF",
  },
  subtitle: {
    marginTop: 6,
    fontSize: 13,
    lineHeight: 19,
    color: "#A7C0E8",
    maxWidth: "95%",
  },
  statsRow: {
    marginTop: 16,
    flexDirection: "row",
    gap: 8,
  },
  statChip: {
    flex: 1,
    borderWidth: StyleSheet.hairlineWidth,
    borderRadius: 12,
    paddingVertical: 10,
    paddingHorizontal: 10,
  },
  statLabel: {
    fontSize: 11,
  },
  statValue: {
    marginTop: 4,
    fontSize: 14,
    fontWeight: "700",
  },
  tile: {
    marginTop: 6,
    borderWidth: StyleSheet.hairlineWidth,
    borderRadius: 16,
    padding: 14,
    overflow: "hidden",
  },
  tileGlow: {
    position: "absolute",
    top: -56,
    right: -48,
    width: 140,
    height: 140,
    borderRadius: 999,
  },
  tileHeader: {
    flexDirection: "row",
    justifyContent: "space-between",
    alignItems: "center",
  },
  titleRow: {
    flexDirection: "row",
    alignItems: "center",
    gap: 10,
  },
  iconBubble: {
    width: 28,
    height: 28,
    borderRadius: 999,
    alignItems: "center",
    justifyContent: "center",
  },
  leagueLogo: {
    width: 20,
    height: 20,
    resizeMode: "contain",
  },
  tileTitle: {
    fontSize: 18,
    fontWeight: "800",
  },
  badge: {
    borderRadius: 999,
    paddingHorizontal: 10,
    paddingVertical: 4,
  },
  badgeText: {
    fontSize: 10,
    fontWeight: "800",
    letterSpacing: 0.4,
  },
  tileSub: {
    marginTop: 10,
    fontSize: 13,
    lineHeight: 18,
  },
  tileFooter: {
    marginTop: 14,
    paddingTop: 10,
    borderTopWidth: StyleSheet.hairlineWidth,
    flexDirection: "row",
    alignItems: "center",
    justifyContent: "space-between",
  },
  tileFooterText: {
    fontSize: 12,
    fontWeight: "600",
  },
  section: {
    marginTop: 14,
    borderWidth: StyleSheet.hairlineWidth,
    borderRadius: 14,
    padding: 14,
  },
  sectionTitle: {
    fontSize: 15,
    fontWeight: "700",
  },
  sectionSub: {
    fontSize: 12,
    marginTop: 6,
    lineHeight: 18,
  },
});
