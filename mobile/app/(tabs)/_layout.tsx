// app/(tabs)/_layout.tsx
import { Tabs, useSegments } from "expo-router";
import { Ionicons } from "@expo/vector-icons";
import { HapticTab } from "@/components/haptic-tab";
import { Colors } from "@/constants/theme";
import { useColorScheme } from "@/hooks/use-color-scheme";
import { PulseHeader } from "@/components/layout/PulseHeader";
import { LiveDataBridge } from "@/components/LiveDataBridge";

type SportKey = "nba" | "pga" | "atp";
type IoniconName = keyof typeof Ionicons.glyphMap;

type TabConfig = {
  name: string;
  title: string;
  icon: IoniconName;
};

const NBA_TABS: TabConfig[] = [
  { name: "props", title: "Props", icon: "stats-chart" },
  { name: "game-betting", title: "Game Bets", icon: "analytics" },
  { name: "live", title: "Live", icon: "radio-outline" },
  { name: "first-basket", title: "First Basket", icon: "basketball" },
  { name: "injuries", title: "Injuries", icon: "medkit" },
  { name: "wowy", title: "WOWY", icon: "people" },
  { name: "trend-chart", title: "Trends", icon: "trending-up" },
  { name: "more", title: "More", icon: "menu" },
  { name: "home", title: "Home", icon: "home" },
];

const PGA_TABS: TabConfig[] = [
  { name: "pga-matchups", title: "Matchup Ratings", icon: "stats-chart" },
  { name: "pga-compare", title: "Pairings Compare", icon: "swap-horizontal" },
  { name: "pga-strokes-gained", title: "Strokes Gained", icon: "speedometer" },
  {
    name: "pga-placement-probabilities",
    title: "Placement Probability",
    icon: "podium",
  },
  {
    name: "pga-simulated-finishes",
    title: "Simulated Finishes",
    icon: "bar-chart",
  },
  {
    name: "pga-simulated-leaderboard",
    title: "Sim Leaderboard",
    icon: "trophy",
  },
  { name: "home", title: "Home", icon: "home" },
];

const ATP_TABS: TabConfig[] = [
  { name: "atp-head-to-head", title: "Head to Head", icon: "people" },
  { name: "atp-compare", title: "Matchup Compare", icon: "analytics" },
  { name: "atp-match-predictor", title: "Match Predictor", icon: "flash" },
  {
    name: "atp-tournament-performance",
    title: "Tournament Performance",
    icon: "trophy",
  },
  { name: "atp-surface-splits", title: "Surface Splits", icon: "grid" },
  { name: "home", title: "Home", icon: "home" },
];

const SPORT_TABS: Record<SportKey, TabConfig[]> = {
  nba: NBA_TABS,
  pga: PGA_TABS,
  atp: ATP_TABS,
};

const HIDDEN_SCREENS = [
  "ladders",
  "nba",
  "pga",
  "atp",
  "bad-lines",
  "live-props-dev",
];

const ALL_TAB_NAMES = [
  ...NBA_TABS,
  ...PGA_TABS,
  ...ATP_TABS,
].map((tab) => tab.name);

const ALL_SCREEN_NAMES = Array.from(
  new Set([...ALL_TAB_NAMES, ...HIDDEN_SCREENS])
);

const isPgaSegment = (segment: string) =>
  segment === "pga" || segment.startsWith("pga-");
const isAtpSegment = (segment: string) =>
  segment === "atp" || segment.startsWith("atp-");

export default function TabLayout() {
  const colorScheme = useColorScheme();
  const segments = useSegments();

  const activeSport: SportKey = segments.some(isPgaSegment)
    ? "pga"
    : segments.some(isAtpSegment)
    ? "atp"
    : "nba";

  const activeTabs = SPORT_TABS[activeSport];
  const activeNames = new Set(activeTabs.map((tab) => tab.name));
  const hiddenScreens = ALL_SCREEN_NAMES.filter(
    (name) => !activeNames.has(name)
  );

  return (
    <>
      {/* 🔥 Live data sync — all platforms */}
      <LiveDataBridge />

      <Tabs
        screenOptions={{
          tabBarActiveTintColor:
            Colors[colorScheme ?? "light"].tint,
          tabBarButton: HapticTab,
          header: () => <PulseHeader />,
        }}
      >
        {activeTabs.map((tab) => (
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
          <Tabs.Screen
            key={name}
            name={name}
            options={{ href: null }}
          />
        ))}
      </Tabs>
    </>
  );
}
