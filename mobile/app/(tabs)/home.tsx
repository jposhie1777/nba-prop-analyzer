// app/(tabs)/home.tsx
import { ScrollView, Text, StyleSheet, Pressable, View } from "react-native";
import { useRouter } from "expo-router";
import { useTheme } from "@/store/useTheme";

type SportTileProps = {
  title: string;
  subtitle: string;
  onPress: () => void;
  badge?: string;
};

function SportTile({ title, subtitle, onPress, badge }: SportTileProps) {
  const { colors } = useTheme();

  return (
    <Pressable
      onPress={onPress}
      style={[
        styles.tile,
        {
          backgroundColor: colors.surface.card,
          borderColor: colors.border.subtle,
        },
      ]}
    >
      <View style={styles.tileHeader}>
        <Text style={[styles.tileTitle, { color: colors.text.primary }]}>
          {title}
        </Text>
        {badge ? (
          <View
            style={[
              styles.badge,
              { backgroundColor: colors.accent.primary },
            ]}
          >
            <Text style={styles.badgeText}>{badge}</Text>
          </View>
        ) : null}
      </View>
      <Text style={[styles.tileSub, { color: colors.text.muted }]}>
        {subtitle}
      </Text>
    </Pressable>
  );
}

export default function Home() {
  const router = useRouter();
  const { colors } = useTheme();

  return (
    <ScrollView
      style={{
        flex: 1,
        backgroundColor: colors.surface.screen,
      }}
      contentContainerStyle={{
        padding: 16,
        paddingBottom: 40,
      }}
    >
      <Text
        style={[
          styles.h1,
          { color: colors.text.primary },
        ]}
      >
        Research Hub
      </Text>
      <Text style={[styles.subtitle, { color: colors.text.muted }]}>
        Choose a sport to open analytics, models, and betting tools.
      </Text>

      <SportTile
        title="NBA"
        subtitle="Props, live edges, team & player analytics"
        badge="LIVE"
        onPress={() => router.push("/(tabs)/nba")}
      />

      <SportTile
        title="PGA"
        subtitle="Course fit, form, placements, and simulations"
        badge="NEW"
        onPress={() => router.push("/(tabs)/pga")}
      />

      <SportTile
        title="ATP"
        subtitle="Surface splits, form, and matchup trends"
        badge="ATP"
        onPress={() => router.push("/(tabs)/atp")}
      />

      <View style={styles.section}>
        <Text style={[styles.sectionTitle, { color: colors.text.primary }]}>
          More Sports
        </Text>
        <Text style={[styles.sectionSub, { color: colors.text.muted }]}>
          This hub supports adding new sports quickly. Ask me to wire up any
          league and I will add it here.
        </Text>
      </View>
    </ScrollView>
  );
}

const styles = StyleSheet.create({
  h1: {
    fontSize: 22,
    fontWeight: "800",
    marginBottom: 6,
  },
  subtitle: {
    fontSize: 13,
    marginBottom: 14,
  },
  tile: {
    borderWidth: StyleSheet.hairlineWidth,
    borderRadius: 14,
    padding: 16,
    marginTop: 12,
  },
  tileHeader: {
    flexDirection: "row",
    alignItems: "center",
    justifyContent: "space-between",
  },
  tileTitle: {
    fontSize: 18,
    fontWeight: "800",
  },
  tileSub: {
    marginTop: 6,
    fontSize: 13,
  },
  badge: {
    paddingHorizontal: 8,
    paddingVertical: 4,
    borderRadius: 999,
  },
  badgeText: {
    color: "#FFFFFF",
    fontSize: 10,
    fontWeight: "800",
    letterSpacing: 0.4,
  },
  section: {
    marginTop: 28,
  },
  sectionTitle: {
    fontSize: 16,
    fontWeight: "700",
  },
  sectionSub: {
    fontSize: 12,
    marginTop: 6,
  },
});