// components/bets/BetSlipModal.tsx
import {
  Modal,
  View,
  Text,
  Pressable,
  StyleSheet,
  FlatList,
} from "react-native";
import * as Clipboard from "expo-clipboard";

import { useTheme } from "@/store/useTheme";
import { useBetsStore } from "@/store/useBetsStore";
import { formatBetsForGambly } from "@/lib/export/gambly";

type Props = {
  visible: boolean;
  onClose: () => void;
};

export function BetSlipModal({ visible, onClose }: Props) {
  const { colors } = useTheme();

  const betsById = useBetsStore((s) => s.betsById);
  const removeBet = useBetsStore((s) => s.removeBet);
  const clearAll = useBetsStore((s) => s.clearAll);

  const bets = Object.values(betsById);

  async function handleExport() {
    const text = formatBetsForGambly(bets);
    await Clipboard.setStringAsync(text);
    console.log("📤 GAMBLY EXPORT\n" + text);
  }

  return (
    <Modal
      visible={visible}
      animationType="slide"
      presentationStyle="pageSheet"
      onRequestClose={onClose}
    >
      <View
        style={[
          styles.container,
          { backgroundColor: colors.surface.screen },
        ]}
      >
        {/* ======================
            HEADER
        ====================== */}
        <View style={styles.header}>
          <Text style={[styles.title, { color: colors.text.primary }]}>
            Bet Slip
          </Text>

          <Pressable onPress={onClose}>
            <Text style={{ color: colors.text.secondary }}>Done</Text>
          </Pressable>
        </View>

        {/* ======================
            EMPTY STATE
        ====================== */}
        {bets.length === 0 && (
          <View style={styles.empty}>
            <Text style={{ color: colors.text.muted }}>
              No bets selected
            </Text>
          </View>
        )}

        {/* ======================
            BET LIST
        ====================== */}
        <FlatList
          data={bets}
          keyExtractor={(b) => b.selectionId}
          contentContainerStyle={{ paddingBottom: 24 }}
          renderItem={({ item }) => (
            <View
              style={[
                styles.betRow,
                { borderColor: colors.border.subtle },
              ]}
            >
              <View style={{ flex: 1 }}>
                <Text
                  style={[
                    styles.market,
                    { color: colors.text.secondary },
                  ]}
                >
                  {item.marketKey}
                </Text>

                <Text
                  style={[
                    styles.selection,
                    { color: colors.text.primary },
                  ]}
                >
                  {item.outcome} {item.line}
                </Text>

                <Text
                  style={[
                    styles.odds,
                    { color: colors.text.muted },
                  ]}
                >
                  {item.book.toUpperCase()} ·{" "}
                  {item.odds > 0 ? `+${item.odds}` : item.odds}
                </Text>
              </View>

              <Pressable
                onPress={() => removeBet(item.selectionId)}
                hitSlop={8}
              >
                <Text style={{ color: colors.accent.danger }}>
                  Remove
                </Text>
              </Pressable>
            </View>
          )}
        />

        {/* ======================
            FOOTER
        ====================== */}
        {bets.length > 0 && (
          <View
            style={[
              styles.footer,
              { borderColor: colors.border.subtle },
            ]}
          >
            <Pressable onPress={clearAll}>
              <Text style={{ color: colors.text.muted }}>
                Clear all
              </Text>
            </Pressable>

            <Pressable
              onPress={handleExport}
              style={[
                styles.confirm,
                { backgroundColor: colors.accent.primary },
              ]}
            >
              <Text
                style={[
                  styles.confirmText,
                  { color: colors.text.inverse },
                ]}
              >
                Copy for Gambly
              </Text>
            </Pressable>
          </View>
        )}
      </View>
    </Modal>
  );
}

const styles = StyleSheet.create({
  container: {
    flex: 1,
  },

  header: {
    paddingHorizontal: 16,
    paddingVertical: 14,
    flexDirection: "row",
    justifyContent: "space-between",
    alignItems: "center",
  },

  title: {
    fontSize: 18,
    fontWeight: "800",
  },

  empty: {
    flex: 1,
    alignItems: "center",
    justifyContent: "center",
  },

  betRow: {
    flexDirection: "row",
    alignItems: "center",
    paddingHorizontal: 16,
    paddingVertical: 12,
    borderBottomWidth: 1,
    gap: 12,
  },

  market: {
    fontSize: 11,
    fontWeight: "700",
  },

  selection: {
    fontSize: 14,
    fontWeight: "800",
  },

  odds: {
    fontSize: 12,
  },

  footer: {
    paddingHorizontal: 16,
    paddingVertical: 12,
    borderTopWidth: 1,
    flexDirection: "row",
    alignItems: "center",
    justifyContent: "space-between",
  },

  confirm: {
    paddingHorizontal: 18,
    paddingVertical: 10,
    borderRadius: 999,
  },

  confirmText: {
    fontSize: 13,
    fontWeight: "800",
    letterSpacing: 0.5,
  },
});