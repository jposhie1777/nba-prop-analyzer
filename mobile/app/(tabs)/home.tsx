import { ScrollView, StyleSheet, Text, View, Pressable, Image } from "react-native";
import { useRouter } from "expo-router";

import { useTheme } from "@/store/useTheme";
import { LEAGUE_LOGOS } from "@/utils/leagueLogos";

type LeagueCardProps = {
  title: string;
  subtitle?: string;
  route?: string;
  logo?: string;
  disabled?: boolean;
};

function LeagueCard({ title, subtitle, route, logo, disabled = false }: LeagueCardProps) {
  const router = useRouter();
  const { colors } = useTheme();

  return (
    <Pressable
      onPress={() => route && router.push(route)}
      disabled={disabled || !route}
      style={[styles.card, { borderColor: colors.border.subtle, opacity: disabled ? 0.65 : 1 }]}
    >
      <View style={styles.cardLeft}>
        {logo ? <Image source={{ uri: logo }} style={styles.logo} /> : null}
        <View>
          <Text style={[styles.cardTitle, { color: colors.text.primary }]}>{title}</Text>
          {subtitle ? <Text style={[styles.cardSub, { color: colors.text.muted }]}>{subtitle}</Text> : null}
        </View>
      </View>
      <Text style={[styles.chevron, { color: colors.text.muted }]}>{disabled ? "Soon" : "→"}</Text>
    </Pressable>
  );
}

export default function Home() {
  const { colors } = useTheme();

  return (
    <ScrollView style={styles.screen} contentContainerStyle={styles.content}>
      <Text style={styles.headline}>Leagues</Text>

      <LeagueCard title="PGA" route="/(tabs)/pga" logo={LEAGUE_LOGOS.PGA} />
      <LeagueCard title="MLB" route="/(tabs)/mlb" logo={LEAGUE_LOGOS.MLB} />
      <LeagueCard title="MLS" route="/(tabs)/mls" logo={LEAGUE_LOGOS.MLS} />
      <LeagueCard title="EPL" route="/(tabs)/epl" logo={LEAGUE_LOGOS.EPL} />
      <LeagueCard title="ATP" route="/(tabs)/atp" logo={LEAGUE_LOGOS.ATP} />
      <LeagueCard
        title="Soccer analytics"
        subtitle="This section will be going away eventually."
        route="/(tabs)/soccer"
      />

      <View style={[styles.noteBox, { borderColor: colors.border.subtle }]}> 
        <Text style={[styles.noteText, { color: colors.text.muted }]}>More leagues coming soon</Text>
      </View>
    </ScrollView>
  );
}

const styles = StyleSheet.create({
  screen: { flex: 1, backgroundColor: "#050A18" },
  content: { padding: 16, gap: 10, paddingBottom: 40 },
  headline: { color: "#E9F2FF", fontSize: 26, fontWeight: "800", marginBottom: 6 },
  card: {
    borderWidth: StyleSheet.hairlineWidth,
    borderRadius: 14,
    backgroundColor: "#0B1529",
    padding: 14,
    flexDirection: "row",
    alignItems: "center",
    justifyContent: "space-between",
  },
  cardLeft: { flexDirection: "row", alignItems: "center", gap: 10 },
  logo: { width: 28, height: 28, borderRadius: 14, backgroundColor: "#111827" },
  cardTitle: { fontSize: 16, fontWeight: "700" },
  cardSub: { fontSize: 12, marginTop: 2 },
  chevron: { fontSize: 14, fontWeight: "700" },
  noteBox: {
    borderWidth: StyleSheet.hairlineWidth,
    borderRadius: 12,
    padding: 12,
    marginTop: 6,
    backgroundColor: "#0A1224",
  },
  noteText: { fontSize: 13 },
});
