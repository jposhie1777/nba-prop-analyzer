import {
  View,
  Text,
  StyleSheet,
  ScrollView,
  Pressable,
  Linking,
  Alert,
} from "react-native";
import { useMemo, useState } from "react";
import * as Clipboard from "expo-clipboard";
import * as Haptics from "expo-haptics";

import { useTheme } from "@/store/useTheme";
import { useSavedBets } from "@/store/useSavedBets";
import { usePropsStore } from "@/store/usePropsStore"; // 🔑 assumes your live props store
import { sendBetsToDiscord } from "@/lib/export/sendToDiscord";
import { useSafeAreaInsets } from "react-native-safe-area-context";

const GAMBLY_URL = "https://www.gambly.com/gambly-bot";

export default function SavedScreen() {
  // 🔹 THEME
  const colors = useTheme((s) => s.colors);
  const styles = useMemo(() => makeStyles(colors), [colors]);

  // 🔹 STORES
  const TAB_BAR_HEIGHT = 56;
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
  const insets = useSafeAreaInsets();
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
  const [sending, setSending] = useState(false);

  const handleSendToGambly = async () => {
    if (!savedProps.length || sending) return;
  
    try {
      setSending(true);
      await sendBetsToDiscord(
        savedProps.map((p) => ({
          // map SavedScreen shape → SavedBet shape
          selectionId: p.id,
          gameId: p.game_id,
          playerId: p.player_id,
          marketKey: p.market,
          outcome: "OVER", // or p.outcome if you have it
          line: p.line,
          odds: p.odds,
          book: p.book,
        }))
      );
  
      Haptics.notificationAsync(
        Haptics.NotificationFeedbackType.Success
      );
    } catch (err) {
      console.error("❌ Failed to send saved bets to Gambly", err);
      Alert.alert(
        "Export failed",
        "Could not send bets to Gambly. Please try again."
      );
    } finally {
      setSending(false);
    }
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
      <View
        style={[
          styles.actionBar,
          { bottom: insets.bottom + TAB_BAR_HEIGHT },
        ]}
      >
        {/* COPY (legacy – keep for now) */}
        <Pressable
          style={styles.copyBtn}
          onPress={handleCopy}
          disabled={sending}
        >
          <Text style={styles.copyText}>Copy Bets</Text>
        </Pressable>
      
        {/* SEND TO GAMBLy (NEW) */}
        <Pressable
          style={[
            styles.gamblyBtn,
            sending && { opacity: 0.6 },
          ]}
          onPress={handleSendToGambly}
          disabled={sending}
        >
          <Text style={styles.gamblyText}>
            {sending ? "Sending…" : "Send to Gambly"}
          </Text>
        </Pressable>
      </View>
    </View>
  );
}
const makeStyles = (colors: any) =>
  StyleSheet.create({
    root: {
      flex: 1,
      backgroundColor: colors.surface.screen,
    },

    scroll: {
      padding: 16,
    },

    empty: {
      flex: 1,
      justifyContent: "center",
      alignItems: "center",
      padding: 32,
    },

    emptyTitle: {
      fontSize: 18,
      fontWeight: "800",
      color: colors.text.primary,
    },

    emptySub: {
      marginTop: 6,
      color: colors.text.secondary,
      textAlign: "center",
    },

    clearAllBtn: {
      alignSelf: "flex-end",
      marginBottom: 10,
    },

    clearAllText: {
      color: colors.accent.danger,
      fontWeight: "800",
    },

    gameBlock: {
      marginBottom: 20,
    },

    gameHeader: {
      fontSize: 12,
      fontWeight: "900",
      letterSpacing: 0.8,
      color: colors.text.muted,
      marginBottom: 8,
      textTransform: "uppercase",
    },

    betRow: {
      flexDirection: "row",
      alignItems: "center",
      backgroundColor: colors.surface.card,
      borderRadius: 12,
      paddingVertical: 10,
      paddingHorizontal: 12,
      marginBottom: 6,
      borderWidth: 1,
      borderColor: colors.border.subtle,
    },

    betLeft: {
      flex: 1,
    },

    player: {
      fontWeight: "700",
      color: colors.text.primary,
    },

    market: {
      marginTop: 2,
      fontWeight: "600",
      color: colors.text.secondary,
      fontSize: 12,
    },

    ou: {
      fontWeight: "900",
      color: colors.accent.primary,
    },

    price: {
      width: 60,
      textAlign: "right",
      fontWeight: "800",
      color: colors.text.primary,
      marginRight: 8,
    },

    removeBtn: {
      paddingHorizontal: 6,
      paddingVertical: 2,
    },

    removeText: {
      fontSize: 16,
      fontWeight: "900",
      color: colors.text.muted,
    },

    actionBar: {
      position: "absolute",
      left: 0,
      right: 0,
      flexDirection: "row",
      gap: 12,
      padding: 12,
      backgroundColor: colors.surface.card,
      borderTopWidth: 1,
      borderTopColor: colors.border.subtle,
      borderTopWidth: 2,
      borderTopColor: "red",
      zIndex: 50,        // ✅ REQUIRED
      elevation: 50,     // ✅ Android
    },

    copyBtn: {
      flex: 1,
      backgroundColor: colors.surface.elevated,
      paddingVertical: 14,
      borderRadius: 14,
      alignItems: "center",
    },

    copyText: {
      color: colors.text.primary,
      fontWeight: "900",
    },

    gamblyBtn: {
      flex: 1,
      backgroundColor: colors.accent.primary,
      paddingVertical: 14,
      borderRadius: 14,
      alignItems: "center",
    },

    gamblyText: {
      color: colors.text.primary,
      fontWeight: "900",
    },
  });
