// app/(tabs)/_layout.tsx
import { Tabs } from "expo-router";
import { Ionicons } from "@expo/vector-icons";
import { HapticTab } from "@/components/haptic-tab";
import { Colors } from "@/constants/theme";
import { useColorScheme } from "@/hooks/use-color-scheme";
import { PulseHeader } from "@/components/layout/PulseHeader";
import { LiveDataBridge } from "@/components/LiveDataBridge";

type IoniconName = keyof typeof Ionicons.glyphMap;

type TabConfig = {
  name: string;
  title: string;
  icon: IoniconName;
};

const STATIC_TABS: TabConfig[] = [
  { name: "atp-bracket", title: "ATP Bracket", icon: "tennisball-outline" },
  { name: "pga-pairings", title: "PGA Pairings", icon: "golf-outline" },
  { name: "pga-compare", title: "PGA Compare", icon: "swap-horizontal" },
  { name: "home", title: "Home Screen", icon: "home" },
  { name: "soccer", title: "Soccer Analyzer", icon: "football-outline" },
];

const HIDDEN_SCREENS = [
  "atp",
  "atp-compare",
  "atp-head-to-head",
  "atp-surface-splits",
  "atp-tournament-performance",
  "bad-lines",
  "epl",
  "laliga",
  "mls",
  "epl/btts",
  "epl/cards",
  "epl/moneylines",
  "epl/standings",
  "epl/team-master-metrics",
  "epl/total-goals",
  "epl/index",
  "laliga/btts",
  "laliga/cards",
  "laliga/index",
  "laliga/moneylines",
  "laliga/standings",
  "laliga/team-master-metrics",
  "laliga/total-goals",
  "mls/btts",
  "mls/cards",
  "mls/index",
  "mls/moneylines",
  "mls/standings",
  "mls/team-master-metrics",
  "mls/total-goals",
  "first-basket",
  "game-betting",
  "injuries",
  "ladders",
  "live",
  "live-props-dev",
  "nba",
  "nba/index",
  "pga",
  "pga-matchups",
  "pga-placement-probabilities",
  "pga-simulated-finishes",
  "pga-simulated-leaderboard",
  "props",
  "soccer/index",
  "soccer/today",
  "trend-chart",
  "wowy",
];

const ALL_SCREEN_NAMES = Array.from(
  new Set([...STATIC_TABS.map((tab) => tab.name), ...HIDDEN_SCREENS])
);

export default function TabLayout() {
  const colorScheme = useColorScheme();
  const activeNames = new Set(STATIC_TABS.map((tab) => tab.name));
  const hiddenScreens = ALL_SCREEN_NAMES.filter((name) => !activeNames.has(name));

  return (
    <>
      {/* 🔥 Live data sync — all platforms */}
      <LiveDataBridge />

      <Tabs
        screenOptions={{
          tabBarActiveTintColor: Colors[colorScheme ?? "light"].tint,
          tabBarButton: HapticTab,
          header: () => <PulseHeader />,
        }}
      >
        {STATIC_TABS.map((tab) => (
          <Tabs.Screen
            key={tab.name}
            name={tab.name}
            options={{
              title: tab.title,
              tabBarIcon: ({ color }) => (
                <Ionicons name={tab.icon} size={22} color={color} />
              ),
            }}
          />
        ))}

        {hiddenScreens.map((name) => (
          <Tabs.Screen key={name} name={name} options={{ href: null }} />
        ))}
      </Tabs>
    </>
  );
}
