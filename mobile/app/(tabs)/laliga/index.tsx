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
    <Pressable
      onPress={() => router.push(route)}
      style={[styles.tile, { borderColor: colors.border.subtle }]}
    >
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

export default function LaLigaHome() {
  const { colors } = useTheme();

  return (
    <ScrollView style={styles.screen} contentContainerStyle={styles.content}>
      <View style={[styles.hero, { borderColor: colors.border.subtle }]}> 
        <Text style={styles.eyebrow}>LALIGA ANALYTICS</Text>
        <Text style={styles.h1}>Match Markets Dashboard</Text>
        <Text style={styles.sub}>
          Ingested from BALLDONTLIE (ALL-STAR) with current + previous season scope.
          Moneylines, BTTS, totals, and cards all share one shared data foundation.
        </Text>
      </View>


      <Tile
        title="Standings"
        subtitle="Rankings table with points, record, and goal differential."
        route="/(tabs)/laliga/standings"
        icon="trophy-outline"
        accent="#A78BFA"
      />
      <Tile
        title="Team Master Metrics"
        subtitle="Full team metrics table with season + rolling splits and card logs."
        route="/(tabs)/laliga/team-master-metrics"
        icon="grid-outline"
        accent="#22D3EE"
      />

      <Tile
        title="Moneylines"
        subtitle="Model-driven fair prices and win percentages."
        route="/(tabs)/laliga/moneylines"
        icon="cash-outline"
        accent="#60A5FA"
      />
      <Tile
        title="Both Teams To Score"
        subtitle="BTTS probability from team scoring + concession rates."
        route="/(tabs)/laliga/btts"
        icon="football-outline"
        accent="#34D399"
      />
      <Tile
        title="Total Goals"
        subtitle="Projected totals, over 2.5 rate, and volatility metrics."
        route="/(tabs)/laliga/total-goals"
        icon="analytics-outline"
        accent="#FBBF24"
      />
      <Tile
        title="Cards"
        subtitle="Yellow/red card rates and team card-points profile."
        route="/(tabs)/laliga/cards"
        icon="warning-outline"
        accent="#F87171"
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
  sub: { color: "#A7C0E8", marginTop: 8, lineHeight: 18, fontSize: 13 },
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
