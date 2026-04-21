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
  { name: "nba", title: "NBA", icon: "basketball-outline" },
  { name: "pga", title: "PGA", icon: "golf-outline" },
  { name: "mlb", title: "MLB", icon: "baseball-outline" },
  { name: "mls", title: "MLS", icon: "football-outline" },
  { name: "epl", title: "EPL", icon: "trophy-outline" },
  { name: "atp", title: "ATP", icon: "tennisball-outline" },
  { name: "home", title: "Home", icon: "home-outline" },
];

const HIDDEN_SCREENS = [
  "atp/match/[matchId]",
  "atp-compare",
  "atp-head-to-head",
  "atp-surface-splits",
  "atp-tournament-performance",
  "bad-lines",
  "atp-bracket",
  "laliga",
  "epl/btts",
  "epl/cards",
  "epl/betting-analytics",
  "epl/moneylines",
  "epl/standings",
  "epl/team-master-metrics",
  "epl/total-goals",
  "epl/match/[matchId]",
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
  "mls/match/[matchId]",
  "mls/moneylines",
  "mls/standings",
  "mls/team-master-metrics",
  "mls/total-goals",
  "mlb/index",
  "mlb/match/[gamePk]",
  "mlb/pitching-props/[gamePk]",
  "mlb/lineup-matchup/[gamePk]",
  "mlb/hr-matchup/[gamePk]",
  "first-basket",
  "game-betting",
  "injuries",
  "ladders",
  "live",
  "live-props-dev",
  "nba/index",
  "nba/hit-rate-matrix",
  "pga-matchups",
  "pga-compare",
  "pga-pairings",
  "pga-placement-probabilities",
  "pga-simulated-finishes",
  "pga-simulated-leaderboard",
  "props",
  "more",
  "home/index",
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
          tabBarStyle: { display: "flex" },
          tabBarHideOnKeyboard: false,
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
