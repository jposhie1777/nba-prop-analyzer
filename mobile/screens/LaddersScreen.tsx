import { useState, useMemo } from "react";
import {
  View,
  Text,
  FlatList,
  StyleSheet,
  ActivityIndicator,
  RefreshControl,
  Pressable,
} from "react-native";
import { useTheme } from "@/store/useTheme";
import { useLadders, Ladder } from "@/hooks/useLadders";
import { LadderCard } from "@/components/ladders/LadderCard";

type Tab = "pre-live" | "live";

export function LaddersScreen() {
  const { colors } = useTheme();
  const { data, loading, error, refetch } = useLadders();
  const [activeTab, setActiveTab] = useState<Tab>("pre-live");

  const { preLiveLadders, liveLadders } = useMemo(() => {
    const preLive: Ladder[] = [];
    const live: Ladder[] = [];

    for (const ladder of data) {
      if (ladder.game_state === "LIVE") {
        live.push(ladder);
      } else if (ladder.game_state === "UPCOMING") {
        preLive.push(ladder);
      }
      // Skip FINAL games
    }

    return { preLiveLadders: preLive, liveLadders: live };
  }, [data]);

  const displayData = activeTab === "pre-live" ? preLiveLadders : liveLadders;

  if (loading && data.length === 0) {
    return (
      <View style={[styles.container, { backgroundColor: colors.surface.background }]}>
        <View style={styles.center}>
          <ActivityIndicator size="large" color={colors.text.secondary} />
          <Text style={[styles.loadingText, { color: colors.text.muted }]}>
            Loading ladders…
          </Text>
        </View>
      </View>
    );
  }

  if (error) {
    return (
      <View style={[styles.container, { backgroundColor: colors.surface.background }]}>
        <View style={styles.center}>
          <Text style={[styles.errorText, { color: colors.text.primary }]}>
            Failed to load ladders
          </Text>
          <Text style={[styles.errorDetail, { color: colors.text.muted }]}>
            {error.message}
          </Text>
        </View>
      </View>
    );
  }

  return (
    <View style={[styles.container, { backgroundColor: colors.surface.background }]}>
      {/* Sub-tabs */}
      <View style={[styles.tabBar, { borderBottomColor: colors.border.subtle }]}>
        <Pressable
          style={[
            styles.tab,
            activeTab === "pre-live" && [
              styles.activeTab,
              { borderBottomColor: colors.text.primary },
            ],
          ]}
          onPress={() => setActiveTab("pre-live")}
        >
          <Text
            style={[
              styles.tabText,
              { color: activeTab === "pre-live" ? colors.text.primary : colors.text.muted },
            ]}
          >
            Pre-Live
          </Text>
          <View style={[styles.badge, { backgroundColor: colors.surface.card }]}>
            <Text style={[styles.badgeText, { color: colors.text.muted }]}>
              {preLiveLadders.length}
            </Text>
          </View>
        </Pressable>

        <Pressable
          style={[
            styles.tab,
            activeTab === "live" && [
              styles.activeTab,
              { borderBottomColor: "#22c55e" },
            ],
          ]}
          onPress={() => setActiveTab("live")}
        >
          <View style={styles.liveIndicator}>
            {liveLadders.length > 0 && <View style={styles.liveDot} />}
            <Text
              style={[
                styles.tabText,
                { color: activeTab === "live" ? "#22c55e" : colors.text.muted },
              ]}
            >
              Live
            </Text>
          </View>
          <View style={[styles.badge, { backgroundColor: colors.surface.card }]}>
            <Text style={[styles.badgeText, { color: colors.text.muted }]}>
              {liveLadders.length}
            </Text>
          </View>
        </Pressable>
      </View>

      {/* Content */}
      {displayData.length === 0 ? (
        <View style={styles.center}>
          <Text style={[styles.emptyText, { color: colors.text.primary }]}>
            No {activeTab === "live" ? "live" : "pre-live"} ladders
          </Text>
          <Text style={[styles.emptyDetail, { color: colors.text.muted }]}>
            {activeTab === "live"
              ? "Check back when games are in progress"
              : "Check back closer to game time"}
          </Text>
        </View>
      ) : (
        <FlatList
          data={displayData}
          keyExtractor={(item) =>
            `${item.game_id}-${item.player_id}-${item.market}`
          }
          renderItem={({ item }) => <LadderCard ladder={item} />}
          contentContainerStyle={styles.list}
          refreshControl={
            <RefreshControl
              refreshing={loading}
              onRefresh={refetch}
              tintColor={colors.text.secondary}
            />
          }
        />
      )}
    </View>
  );
}

const styles = StyleSheet.create({
  container: {
    flex: 1,
  },
  tabBar: {
    flexDirection: "row",
    borderBottomWidth: 1,
    paddingHorizontal: 12,
  },
  tab: {
    flexDirection: "row",
    alignItems: "center",
    paddingVertical: 12,
    paddingHorizontal: 16,
    marginRight: 8,
    borderBottomWidth: 2,
    borderBottomColor: "transparent",
  },
  activeTab: {
    borderBottomWidth: 2,
  },
  tabText: {
    fontSize: 15,
    fontWeight: "600",
  },
  badge: {
    marginLeft: 8,
    paddingHorizontal: 8,
    paddingVertical: 2,
    borderRadius: 10,
  },
  badgeText: {
    fontSize: 12,
    fontWeight: "600",
  },
  liveIndicator: {
    flexDirection: "row",
    alignItems: "center",
  },
  liveDot: {
    width: 8,
    height: 8,
    borderRadius: 4,
    backgroundColor: "#22c55e",
    marginRight: 6,
  },
  list: {
    padding: 12,
  },
  center: {
    flex: 1,
    alignItems: "center",
    justifyContent: "center",
    padding: 20,
  },
  loadingText: {
    marginTop: 12,
    fontSize: 14,
  },
  errorText: {
    fontSize: 18,
    fontWeight: "600",
    marginBottom: 8,
  },
  errorDetail: {
    fontSize: 14,
    textAlign: "center",
  },
  emptyText: {
    fontSize: 18,
    fontWeight: "600",
    marginBottom: 8,
  },
  emptyDetail: {
    fontSize: 14,
    textAlign: "center",
  },
});
