import { ScrollView, Text, View, Pressable, StyleSheet } from "react-native";
import { useRouter } from "expo-router";
import { Ionicons } from "@expo/vector-icons";

import { useTheme } from "@/store/useTheme";

type TileProps = {
  title: string;
  subtitle: string;
  route: string;
  icon: keyof typeof Ionicons.glyphMap;
  accent: string;
};

function Tile({ title, subtitle, route, icon, accent }: TileProps) {
  const { colors } = useTheme();
  const router = useRouter();

  return (
    <Pressable onPress={() => router.push(route)} style={[styles.tile, { borderColor: colors.border.subtle }]}>
      <View style={[styles.iconBubble, { backgroundColor: `${accent}20` }]}>
        <Ionicons name={icon} size={16} color={accent} />
      </View>
      <View style={{ flex: 1 }}>
        <Text style={[styles.tileTitle, { color: colors.text.primary }]}>{title}</Text>
        <Text style={[styles.tileSub, { color: colors.text.muted }]}>{subtitle}</Text>
      </View>
      <Ionicons name="chevron-forward" size={16} color={colors.text.muted} />
    </Pressable>
  );
}

export default function SoccerHome() {
  const { colors } = useTheme();

  return (
    <ScrollView style={styles.screen} contentContainerStyle={styles.content}>
      <View style={[styles.hero, { borderColor: colors.border.subtle }]}>
        <Text style={styles.eyebrow}>SOCCER ANALYTICS</Text>
        <Text style={styles.h1}>Today&apos;s Matchup Bets</Text>
        <Text style={[styles.sub, { color: colors.text.muted }]}>
          Analyze today&apos;s slate using betting-analysis table signals, then save recommended bets to the Gambly-ready betslip drawer.
        </Text>
      </View>

      <Tile
        title="Today's Matchup Analyzer"
        subtitle="Top recommended bets from outright winner, alternate totals, BTTS, Draw No Bet, and Double Chance markets."
        route="/(tabs)/soccer/today"
        icon="football-outline"
        accent="#22D3EE"
      />
    </ScrollView>
  );
}

const styles = StyleSheet.create({
  screen: { flex: 1, backgroundColor: "#050A18" },
  content: { padding: 16, gap: 10, paddingBottom: 40 },
  hero: {
    borderWidth: StyleSheet.hairlineWidth,
    borderRadius: 16,
    backgroundColor: "#071731",
    padding: 16,
    marginBottom: 8,
  },
  eyebrow: { color: "#90B3E9", fontSize: 11, fontWeight: "700" },
  h1: { color: "#E9F2FF", fontSize: 24, fontWeight: "800", marginTop: 8 },
  sub: { marginTop: 8, lineHeight: 18, fontSize: 13 },
  tile: {
    borderWidth: StyleSheet.hairlineWidth,
    borderRadius: 14,
    backgroundColor: "#0B1529",
    padding: 14,
    flexDirection: "row",
    alignItems: "center",
    gap: 10,
  },
  iconBubble: { width: 30, height: 30, borderRadius: 999, alignItems: "center", justifyContent: "center" },
  tileTitle: { fontSize: 15, fontWeight: "700" },
  tileSub: { fontSize: 12, marginTop: 4 },
});
