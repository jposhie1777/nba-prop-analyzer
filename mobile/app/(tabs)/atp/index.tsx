// app/(tabs)/atp/index.tsx
import { ScrollView, Text, StyleSheet, View } from "react-native";
import { useTheme } from "@/store/useTheme";

type CapabilityProps = {
  title: string;
  subtitle: string;
};

function Capability({ title, subtitle }: CapabilityProps) {
  const { colors } = useTheme();

  return (
    <View
      style={[
        styles.card,
        {
          backgroundColor: colors.surface.card,
          borderColor: colors.border.subtle,
        },
      ]}
    >
      <Text style={[styles.cardTitle, { color: colors.text.primary }]}>
        {title}
      </Text>
      <Text style={[styles.cardSub, { color: colors.text.muted }]}>
        {subtitle}
      </Text>
    </View>
  );
}

function Bullet({ text }: { text: string }) {
  const { colors } = useTheme();
  return (
    <Text style={[styles.bullet, { color: colors.text.muted }]}>
      - {text}
    </Text>
  );
}

export default function AtpHome() {
  const { colors } = useTheme();

  return (
    <ScrollView
      style={{ flex: 1, backgroundColor: colors.surface.screen }}
      contentContainerStyle={{ padding: 16, paddingBottom: 40 }}
    >
      <Text style={[styles.h1, { color: colors.text.primary }]}>
        ATP All-Star Analytics
      </Text>
      <Text style={[styles.sub, { color: colors.text.muted }]}>
        With All-Star tier access, we can turn ATP matches, rankings,
        tournaments, and race standings into betting analytics. This tier does
        not include match statistics or betting odds feeds.
      </Text>

      <Text style={[styles.sectionTitle, { color: colors.text.primary }]}>
        Available data (All-Star)
      </Text>
      <View style={styles.section}>
        <Bullet text="Players, tournaments, and rankings." />
        <Bullet text="Match results with score, round, surface, and duration." />
        <Bullet text="ATP Race standings for the current season." />
      </View>

      <Text style={[styles.sectionTitle, { color: colors.text.primary }]}>
        Betting analytics we can build
      </Text>
      <Capability
        title="Court type analytics"
        subtitle="Win rates, straight-sets rates, and average sets by surface."
      />
      <Capability
        title="Head-to-head history"
        subtitle="Series record, last meeting, and surface splits from match results."
      />
      <Capability
        title="Recent form and momentum"
        subtitle="Last N matches, streaks, and trend splits by surface or round."
      />
      <Capability
        title="Tournament performance"
        subtitle="Round reach and results by category (Grand Slam, Masters, 500/250)."
      />
      <Capability
        title="Ranking and race context"
        subtitle="Ranking movement, points pace, and race position snapshots."
      />
      <Capability
        title="Matchup previews"
        subtitle="Model-based projections using surface, form, and ranking gaps."
      />

      <Text style={[styles.sectionTitle, { color: colors.text.primary }]}>
        What is not in All-Star
      </Text>
      <View style={styles.section}>
        <Bullet text="Match statistics (serve, return, break points, aces)." />
        <Bullet text="Player career stat endpoints or official H2H endpoint." />
        <Bullet text="Betting odds feeds." />
      </View>
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
  sectionTitle: {
    fontSize: 14,
    fontWeight: "700",
    marginTop: 12,
    marginBottom: 6,
  },
  section: {
    marginBottom: 6,
  },
  card: {
    borderWidth: StyleSheet.hairlineWidth,
    borderRadius: 12,
    padding: 14,
    marginTop: 10,
  },
  cardTitle: {
    fontSize: 15,
    fontWeight: "800",
  },
  cardSub: {
    marginTop: 4,
    fontSize: 12,
  },
  bullet: {
    fontSize: 12,
    marginTop: 4,
  },
});
