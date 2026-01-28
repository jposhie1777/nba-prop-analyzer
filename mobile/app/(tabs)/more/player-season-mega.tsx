// app/(tabs)/more/player-season-mega.tsx
import { View, Text } from "react-native";
import { SafeAreaView } from "react-native-safe-area-context";
import { useEffect, useState } from "react";
import Constants from "expo-constants";

import { PlayerSeasonMegaTable } from "@/components/table/PlayerSeasonMegaTable";
import { useTheme } from "@/store/useTheme";

const API = Constants.expoConfig?.extra?.API_URL!;

export default function PlayerSeasonMegaScreen() {
  const { colors } = useTheme();

  const [rows, setRows] = useState<any[]>([]);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    fetch(`${API}/players/season-mega`)
      .then((r) => r.json())
      .then((data) => {
        // 🔍 IMPORTANT: normalize shape
        if (Array.isArray(data)) {
          setRows(data);
        } else if (Array.isArray(data?.rows)) {
          setRows(data.rows);
        } else {
          setRows([]);
        }
      })
      .catch((e) => {
        console.error(e);
        setError("Failed to load data");
      });
  }, []);

  return (
    <SafeAreaView
      style={{
        flex: 1,
        backgroundColor: colors.surface.screen,
      }}
    >
      <View style={{ flex: 1, padding: 12 }}>
        {error && (
          <Text style={{ color: colors.text.danger }}>
            {error}
          </Text>
        )}

        {!rows.length ? (
          <Text style={{ color: colors.text.muted }}>
            No data returned
          </Text>
        ) : (
          <PlayerSeasonMegaTable rows={rows} />
        )}
      </View>
    </SafeAreaView>
  );
}
