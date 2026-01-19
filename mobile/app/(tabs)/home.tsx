import { ScrollView, View, Text, StyleSheet, Pressable } from "react-native";
import { useRouter } from "expo-router";
import { useTheme } from "@/store/useTheme";

function Tile({ title, subtitle, onPress }: any) {
  const { colors } = useTheme();
  return (
    <Pressable
      onPress={onPress}
      style={[styles.tile, { borderColor: colors.border }]}
    >
      <Text style={[styles.tileTitle, { color: colors.text }]}>{title}</Text>
      {!!subtitle && (
        <Text style={[styles.tileSub, { color: colors.mutedText }]}>
          {subtitle}
        </Text>
      )}
    </Pressable>
  );
}

export default function Home() {
  const router = useRouter();
  const { colors } = useTheme();

  return (
    <ScrollView
      style={{ flex: 1, backgroundColor: colors.background }}
      contentContainerStyle={{ padding: 16, paddingBottom: 40 }}
    >
      <Text style={[styles.h1, { color: colors.text }]}>Today</Text>
      <View style={[styles.card, { borderColor: colors.border }]}>
        <Text style={{ color: colors.mutedText }}>
          (Coming next) Brief overview of today’s NBA games.
        </Text>
      </View>

      <Text style={[styles.h1, { color: colors.text, marginTop: 18 }]}>
        Directory
      </Text>

      <Tile
        title="Player Props"
        subtitle="Lines, odds, confidence"
        onPress={() => router.push("/(tabs)/props")}
      />
      <Tile
        title="Live"
        subtitle="Live scores, odds, box"
        onPress={() => router.push("/(tabs)/live")}
      />
      <Tile
        title="First Basket"
        subtitle="Matchups + projections"
        onPress={() => router.push("/(tabs)/first-basket")}
      />
      <Tile
        title="Trend Chart"
        subtitle="Player / market trends"
        onPress={() => router.push("/(tabs)/trends")}
      />
      <Text style={[styles.h1, { color: colors.text, marginTop: 24 }]}>
        More
      </Text>
      <Tile
        title="Lineups"
        subtitle="Projected & most common"
        onPress={() => router.push("/(more)/lineups")}
      />

      <Tile
        title="Teams"
        subtitle="Team profiles & stats"
        onPress={() => router.push("/(more)/teams")}
      />

      <Tile
        title="Saved"
        subtitle="Saved bets & picks"
        onPress={() => router.push("/(more)/SavedScreen")}
      />

      <Tile
        title="Explore"
        subtitle="All features"
        onPress={() => router.push("/(more)/explore")}
      />
    </ScrollView>
  );
}

const styles = StyleSheet.create({
  h1: { fontSize: 18, fontWeight: "800", marginBottom: 10 },
  card: {
    borderWidth: StyleSheet.hairlineWidth,
    borderRadius: 12,
    padding: 14,
  },
  tile: {
    borderWidth: StyleSheet.hairlineWidth,
    borderRadius: 12,
    padding: 14,
    marginTop: 10,
  },
  tileTitle: { fontSize: 16, fontWeight: "800" },
  tileSub: { marginTop: 4, fontSize: 12 },
});
