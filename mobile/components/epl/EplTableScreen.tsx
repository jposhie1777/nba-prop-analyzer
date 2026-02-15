import { useEffect, useMemo, useState } from "react";
import { ActivityIndicator, FlatList, Image, ScrollView, StyleSheet, Text, View } from "react-native";
import Constants from "expo-constants";
import { useTheme } from "@/store/useTheme";

type Column = { key: string; label: string };

type Props = {
  endpoint: string;
  title: string;
  subtitle: string;
  columns: Column[];
};

const API = Constants.expoConfig?.extra?.API_URL || "";

function formatCell(value: unknown): string {
  if (value == null) return "-";
  if (typeof value === "number") return Number.isInteger(value) ? `${value}` : value.toFixed(2);
  return String(value);
}

export function EplTableScreen({ endpoint, title, subtitle, columns }: Props) {
  const { colors } = useTheme();
  const [rows, setRows] = useState<any[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    let alive = true;
    setLoading(true);
    fetch(`${API}${endpoint}`)
      .then((r) => r.json())
      .then((data) => {
        if (!alive) return;
        setRows(Array.isArray(data) ? data : []);
      })
      .catch((e) => {
        if (!alive) return;
        setError(String(e));
      })
      .finally(() => {
        if (alive) setLoading(false);
      });

    return () => {
      alive = false;
    };
  }, [endpoint]);

  const header = useMemo(
    () => (
      <View style={[styles.hero, { borderColor: colors.border.subtle }]}> 
        <Text style={styles.eyebrow}>EPL</Text>
        <Text style={styles.h1}>{title}</Text>
        <Text style={[styles.sub, { color: colors.text.muted }]}>{subtitle}</Text>
      </View>
    ),
    [colors.border.subtle, colors.text.muted, subtitle, title]
  );

  if (loading) {
    return (
      <View style={[styles.center, { backgroundColor: "#050A18" }]}>
        <ActivityIndicator color={colors.accent.primary} />
      </View>
    );
  }

  if (error) {
    return (
      <View style={[styles.center, { backgroundColor: "#050A18", padding: 16 }]}>
        <Text style={{ color: "#fff" }}>{error}</Text>
      </View>
    );
  }

  return (
    <FlatList
      style={{ flex: 1, backgroundColor: "#050A18" }}
      contentContainerStyle={{ padding: 12, paddingBottom: 30, gap: 10 }}
      ListHeaderComponent={header}
      data={rows}
      keyExtractor={(item, idx) => `${item.match_id || item.team_name || idx}`}
      renderItem={({ item }) => {
        const homeLogo = item.home_logo as string | undefined;
        const awayLogo = item.away_logo as string | undefined;
        return (
          <View style={[styles.card, { borderColor: colors.border.subtle }]}> 
            {(item.home_team || item.team_name) && (
              <View style={styles.titleRow}>
                {homeLogo ? <Image source={{ uri: homeLogo }} style={styles.logo} /> : null}
                <Text style={[styles.teamText, { color: colors.text.primary }]}>
                  {item.home_team && item.away_team
                    ? `${item.home_team} vs ${item.away_team}`
                    : item.team_name}
                </Text>
                {awayLogo ? <Image source={{ uri: awayLogo }} style={styles.logo} /> : null}
              </View>
            )}
            <ScrollView horizontal showsHorizontalScrollIndicator={false}>
              <View style={{ gap: 6 }}>
                {columns.map((col) => (
                  <View key={col.key} style={styles.row}>
                    <Text style={[styles.key, { color: colors.text.muted }]}>{col.label}</Text>
                    <Text style={[styles.value, { color: colors.text.primary }]}>{formatCell(item[col.key])}</Text>
                  </View>
                ))}
              </View>
            </ScrollView>
          </View>
        );
      }}
    />
  );
}

const styles = StyleSheet.create({
  center: { flex: 1, justifyContent: "center", alignItems: "center" },
  hero: {
    borderWidth: StyleSheet.hairlineWidth,
    borderRadius: 16,
    backgroundColor: "#071731",
    padding: 16,
    marginBottom: 8,
  },
  eyebrow: { color: "#90B3E9", fontSize: 11, fontWeight: "700" },
  h1: { color: "#E9F2FF", fontSize: 22, fontWeight: "800", marginTop: 8 },
  sub: { marginTop: 8, fontSize: 12 },
  card: {
    borderWidth: StyleSheet.hairlineWidth,
    borderRadius: 14,
    backgroundColor: "#0B1529",
    padding: 12,
    gap: 8,
  },
  titleRow: { flexDirection: "row", alignItems: "center", gap: 8 },
  logo: { width: 20, height: 20, borderRadius: 10, backgroundColor: "#fff" },
  teamText: { fontWeight: "700", fontSize: 14 },
  row: { flexDirection: "row", justifyContent: "space-between", minWidth: 290, gap: 10 },
  key: { fontSize: 12 },
  value: { fontSize: 12, fontWeight: "700" },
});
