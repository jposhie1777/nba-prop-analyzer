import {
  View,
  Text,
  StyleSheet,
  ScrollView,
  Pressable,
  Linking,
  Alert,
} from "react-native";
import { useMemo } from "react";
import * as Clipboard from "expo-clipboard";
import * as Haptics from "expo-haptics";

import colors from "../../theme/color";
import { useSavedBets } from "@/store/useSavedBets";
import { usePropsStore } from "@/store/usePropsStore"; // 🔑 assumes your live props store

const GAMBLY_URL = "https://www.gambly.com/gambly-bot";

export default function SavedScreen() {
  // ---------------------------
  // GLOBAL STORES
  // ---------------------------
  const savedIds = useSavedBets((s) => s.savedIds);
  const toggleSave = useSavedBets((s) => s.toggleSave);
  const clearAll = useSavedBets((s) => s.clearAll);

  const allProps = usePropsStore((s) => s.props); // live props already loaded elsewhere

  // ---------------------------
  // DERIVE SAVED PROPS
  // ---------------------------
  const savedProps = useMemo(() => {
    if (!savedIds.size) return [];
    return allProps.filter((p) => savedIds.has(p.id));
  }, [allProps, savedIds]);

  // ---------------------------
  // GROUP BY GAME
  // ---------------------------
  const grouped = useMemo(() => {
    const map = new Map<string, typeof savedProps>();

    savedProps.forEach((p) => {
      const key = p.matchup ?? "Other";
      if (!map.has(key)) map.set(key, []);
      map.get(key)!.push(p);
    });

    return Array.from(map.entries());
  }, [savedProps]);

  // ---------------------------
  // GAMBLY COPY FORMAT
  // ---------------------------
  const gamblyText = useMemo(() => {
    return savedProps
      .map(
        (p) =>
          `${p.player} ${p.market} O ${p.line} ${p.odds}`
      )
      .join("\n");
  }, [savedProps]);

  const handleCopy = async () => {
    if (!gamblyText) return;
    await Clipboard.setStringAsync(gamblyText);
    Haptics.notificationAsync(
      Haptics.NotificationFeedbackType.Success
    );
  };

  const openGambly = () => {
    Linking.openURL(GAMBLY_URL);
  };

  const confirmClearAll = () => {
    Alert.alert(
      "Clear all saved bets?",
      "This will permanently remove all saved bets.",
      [
        { text: "Cancel", style: "cancel" },
        {
          text: "Clear All",
          style: "destructive",
          onPress: clearAll,
        },
      ]
    );
  };

  // ---------------------------
  // EMPTY STATE
  // ---------------------------
  if (savedProps.length === 0) {
    return (
      <View style={styles.empty}>
        <Text style={styles.emptyTitle}>No saved bets</Text>
        <Text style={styles.emptySub}>
          Save props from the Home tab to export them here.
        </Text>
      </View>
    );
  }

  // ---------------------------
  // RENDER
  // ---------------------------
  return (
    <View style={styles.root}>
      <ScrollView contentContainerStyle={styles.scroll}>
        {/* CLEAR ALL */}
        <Pressable onPress={confirmClearAll} style={styles.clearAllBtn}>
          <Text style={styles.clearAllText}>Clear All</Text>
        </Pressable>

        {grouped.map(([matchup, bets]) => (
          <View key={matchup} style={styles.gameBlock}>
            <Text style={styles.gameHeader}>{matchup}</Text>

            {bets.map((bet) => (
              <View key={bet.id} style={styles.betRow}>
                <View style={styles.betLeft}>
                  <Text style={styles.player}>{bet.player}</Text>
                  <Text style={styles.market}>
                    {bet.market} <Text style={styles.ou}>O</Text> {bet.line}
                  </Text>
                </View>

                <Text style={styles.price}>{bet.odds}</Text>

                {/* REMOVE */}
                <Pressable
                  onPress={() => toggleSave(bet.id)}
                  hitSlop={12}
                  style={styles.removeBtn}
                >
                  <Text style={styles.removeText}>✕</Text>
                </Pressable>
              </View>
            ))}
          </View>
        ))}

        <View style={{ height: 90 }} />
      </ScrollView>

      {/* ACTION BAR */}
      <View style={styles.actionBar}>
        <Pressable style={styles.copyBtn} onPress={handleCopy}>
          <Text style={styles.copyText}>Copy Bets</Text>
        </Pressable>

        <Pressable style={styles.gamblyBtn} onPress={openGambly}>
          <Text style={styles.gamblyText}>Open Gambly Bot</Text>
        </Pressable>
      </View>
    </View>
  );
}