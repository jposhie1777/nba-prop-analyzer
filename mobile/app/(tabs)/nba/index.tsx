// app/(tabs)/nba/index.tsx
import { useState } from "react";
import { Pressable, StyleSheet, Text, View } from "react-native";

import { HitRateMatrixContent } from "./hit-rate-matrix";
import { NbaResearchScreen } from "@/components/nba/NbaResearchScreen";

type Tab = "research" | "hit-rate-matrix";

const TABS: { key: Tab; label: string }[] = [
  { key: "research", label: "Research" },
  { key: "hit-rate-matrix", label: "Hit Rate Matrix" },
];

export default function NbaHomeScreen() {
  const [activeTab, setActiveTab] = useState<Tab>("research");

  return (
    <View style={styles.screen}>
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

      <View style={styles.content}>
        {activeTab === "research" ? <NbaResearchScreen /> : null}
        {activeTab === "hit-rate-matrix" ? <HitRateMatrixContent /> : null}
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
  tabActive: { borderBottomColor: "#A855F7" },
  tabInactive: { borderBottomColor: "transparent" },
  tabText: { fontSize: 12, fontWeight: "800" },
  tabTextActive: { color: "#A855F7" },
  tabTextInactive: { color: "#64748B" },
  content: { flex: 1 },
});
