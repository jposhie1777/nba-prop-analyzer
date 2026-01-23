// /screens/liveGamesScreen.tsx
import { FlatList, View, Text } from "react-native";
import { Pressable } from "react-native";
import { useState } from "react";
import * as Linking from "expo-linking";
import * as Clipboard from "expo-clipboard";

import { useTheme } from "@/store/useTheme";
import { LiveGameCard } from "@/components/live/LiveGameCard";
import { useLiveGames } from "@/hooks/useLiveGames";
import { useLivePlayerStats } from "@/hooks/useLivePlayerStats";
import { useSavedBets } from "@/store/useSavedBets";

/* ============================
   HELPERS
============================ */

function formatBetLine(bet: any) {
  if (bet.side === "milestone") {
    return `${bet.line}+`;
  }

  if (!bet.side) {
    return `${bet.line}`;
  }

  return `${bet.side.toUpperCase()} ${bet.line}`;
}

/* ============================
   SCREEN
============================ */

export default function LiveGamesScreen() {
  const { colors } = useTheme();
  const clearAll = useSavedBets((s) => s.clearAll);
  const { games, mode } = useLiveGames();
  const { playersByGame } = useLivePlayerStats();

  const [betslipOpen, setBetslipOpen] = useState(false);
  const savedIds = useSavedBets((s) => s.savedIds);
  const bets = useSavedBets((s) => s.bets);

  const isConnecting = mode === "sse" && games.length === 0;

  const copyAllBets = async () => {
    if (bets.size === 0) return;

    const text = Array.from(bets.values())
      .map((bet) => {
        const line = formatBetLine(bet);
        const odds = bet.odds != null ? ` (${bet.odds})` : "";
        return `${bet.player} ${bet.market} ${line}${odds}`;
      })
      .join("\n");

    await Clipboard.setStringAsync(text);
  };

  /* ===========================
     DEV GUARD (TERMINAL ONLY)
  =========================== */
  const guardedPlayersByGame = (gameId: string) => {
    const players = playersByGame(gameId);

    if (__DEV__) {
      if (players.length === 0) {
        console.warn("🟠 GUARD: no players for game", gameId);
      } else {
        const teams = new Set(players.map((p) => p.team));
        if (teams.size < 2) {
          console.warn("🔴 GUARD: players not split by team", {
            gameId,
            teams: Array.from(teams),
            samplePlayer: players[0],
          });
        }
      }
    }

    return players;
  };

  return (
    <View style={{ flex: 1, backgroundColor: colors.surface.screen }}>
      {/* =====================
          LIVE GAMES
      ===================== */}
      <FlatList
        data={games}
        keyExtractor={(g) => g.gameId}
        contentContainerStyle={{ paddingBottom: 140 }}
        keyboardShouldPersistTaps="handled"
        renderItem={({ item }) => (
          <LiveGameCard
            game={item}
            players={guardedPlayersByGame(item.gameId)}
          />
        )}
      />

      {/* =====================
          BETSLIP BAR
      ===================== */}
      {savedIds.size > 0 && (
        <View
          pointerEvents="box-none"
          style={{
            position: "absolute",
            bottom: 0,
            left: 0,
            right: 0,
            alignItems: "center",
          }}
        >
          <Pressable
            pointerEvents="auto"
            onPress={() => setBetslipOpen(true)}
            style={{
              marginBottom: 16,
              width: "90%",
              backgroundColor: colors.accent.primary,
              borderRadius: 16,
              paddingVertical: 14,
              alignItems: "center",
            }}
          >
            <Text
              style={{
                color: colors.text.primary,
                fontWeight: "800",
                fontSize: 14,
              }}
            >
              View Betslip ({savedIds.size})
            </Text>
          </Pressable>
        </View>
      )}

      {/* =====================
          BETSLIP DRAWER
      ===================== */}
      {betslipOpen && (
        <View
          style={{
            position: "absolute",
            left: 0,
            right: 0,
            bottom: 0,
            backgroundColor: colors.surface.card,
            borderTopLeftRadius: 20,
            borderTopRightRadius: 20,
            padding: 16,
            maxHeight: "70%",
          }}
        >
          <Text
            style={{
              fontWeight: "800",
              fontSize: 16,
              marginBottom: 12,
              color: colors.text.primary,
            }}
          >
            Betslip
          </Text>

          {Array.from(bets.values()).map((bet) => (
            <View key={bet.id} style={{ marginBottom: 10 }}>
              <Text style={{ fontWeight: "700" }}>
                {bet.player} · {bet.market}
              </Text>

              <Text style={{ fontSize: 12, color: colors.text.muted }}>
                {formatBetLine(bet)}
                {bet.odds != null ? ` (${bet.odds})` : ""}
              </Text>
            </View>
          ))}

          <View style={{ gap: 12, marginTop: 16 }}>
            <Pressable
              onPress={() => Linking.openURL("https://www.gambly.com/")}
              style={{
                paddingVertical: 12,
                borderRadius: 10,
                backgroundColor: "#5865F2",
                alignItems: "center",
              }}
            >
              <Text style={{ fontWeight: "800", color: "#fff" }}>
                Open Gambly
              </Text>
            </Pressable>

            <Pressable
              onPress={copyAllBets}
              style={{
                paddingVertical: 12,
                borderRadius: 10,
                backgroundColor: colors.surface.subtle,
                alignItems: "center",
              }}
            >
              <Text style={{ fontWeight: "700", color: colors.text.primary }}>
                Copy All Bets
              </Text>
            </Pressable>

            <View style={{ flexDirection: "row", gap: 12 }}>
              <Pressable
                onPress={clearAll}
                style={{
                  flex: 1,
                  paddingVertical: 12,
                  borderRadius: 10,
                  backgroundColor: colors.surface.subtle,
                  alignItems: "center",
                }}
              >
                <Text style={{ fontWeight: "700", color: colors.text.primary }}>
                  Clear All
                </Text>
              </Pressable>

              <Pressable
                onPress={() => setBetslipOpen(false)}
                style={{
                  flex: 1,
                  paddingVertical: 12,
                  borderRadius: 10,
                  backgroundColor: colors.accent.primary,
                  alignItems: "center",
                }}
              >
                <Text style={{ fontWeight: "700", color: colors.text.primary }}>
                  Close
                </Text>
              </Pressable>
            </View>
          </View>
        </View>
      )}

      {/* =====================
          EMPTY STATE
      ===================== */}
      {games.length === 0 && !isConnecting && (
        <View style={{ padding: 24, alignItems: "center" }}>
          <Text style={{ color: colors.text.muted }}>
            No live games right now
          </Text>
        </View>
      )}
    </View>
  );
}