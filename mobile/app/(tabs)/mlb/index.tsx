import { useState } from "react";
import { Pressable, StyleSheet, Text, View } from "react-native";

import { MlbUpcomingGamesScreen } from "@/components/mlb/MlbUpcomingGamesScreen";
import { MlbHrCheatSheetScreen } from "@/components/mlb/MlbHrCheatSheetScreen";
import { MlbNrfiScreen } from "@/components/mlb/MlbNrfiScreen";
import { MlbPitchingSummaryScreen } from "@/components/mlb/MlbPitchingSummaryScreen";

type Tab = "matchups" | "pitching" | "cheat-sheet" | "nrfi";

const TABS: { key: Tab; label: string }[] = [
  { key: "matchups", label: "Matchups" },
  { key: "pitching", label: "Pitching" },
  { key: "cheat-sheet", label: "HR Cheat Sheet" },
  { key: "nrfi", label: "NRFI / YRFI" },
];

export default function MlbHomeScreen() {
  const [activeTab, setActiveTab] = useState<Tab>("matchups");

  return (
    <View style={styles.screen}>
      {/* Tab bar */}
      <View style={styles.tabBar}>
        {TABS.map((tab) => (
          <Pressable
            key={tab.key}
            style={[styles.tab, activeTab === tab.key ? styles.tabActive : styles.tabInactive]}
            onPress={() => setActiveTab(tab.key)}
          >
            <Text
              style={[
                styles.tabText,
                activeTab === tab.key ? styles.tabTextActive : styles.tabTextInactive,
              ]}
              numberOfLines={1}
            >
              {tab.label}
            </Text>
          </Pressable>
        ))}
      </View>

      {/* Content */}
      <View style={styles.content}>
        {activeTab === "matchups" ? <MlbUpcomingGamesScreen /> : null}
        {activeTab === "pitching" ? <MlbPitchingSummaryScreen /> : null}
        {activeTab === "cheat-sheet" ? <MlbHrCheatSheetScreen /> : null}
        {activeTab === "nrfi" ? <MlbNrfiScreen /> : null}
      </View>
    </View>
  );
}

const styles = StyleSheet.create({
  screen: { flex: 1, backgroundColor: "#050A18" },
  tabBar: {
    flexDirection: "row",
    backgroundColor: "#0B1529",
    borderBottomWidth: StyleSheet.hairlineWidth,
    borderBottomColor: "#1E293B",
  },
  tab: {
    flex: 1,
    paddingVertical: 12,
    alignItems: "center",
    borderBottomWidth: 2,
  },
  tabActive: { borderBottomColor: "#10B981" },
  tabInactive: { borderBottomColor: "transparent" },
  tabText: { fontSize: 11, fontWeight: "800" },
  tabTextActive: { color: "#10B981" },
  tabTextInactive: { color: "#64748B" },
  content: { flex: 1 },
});
