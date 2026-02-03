// app/(tabs)/_layout.tsx
import { Tabs } from "expo-router";
import { Ionicons } from "@expo/vector-icons";
import { HapticTab } from "@/components/haptic-tab";
import { Colors } from "@/constants/theme";
import { useColorScheme } from "@/hooks/use-color-scheme";
import { PulseHeader } from "@/components/layout/PulseHeader";
import { LiveDataBridge } from "@/components/LiveDataBridge";

export default function TabLayout() {
  const colorScheme = useColorScheme();

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
        <Tabs.Screen
          name="home"
          options={{
            title: "Home",
            tabBarIcon: ({ color }) => (
              <Ionicons name="home" size={22} color={color} />
            ),
          }}
        />

        <Tabs.Screen
          name="props"
          options={{
            title: "Props",
            tabBarIcon: ({ color }) => (
              <Ionicons
                name="stats-chart"
                size={22}
                color={color}
              />
            ),
          }}
        />

        <Tabs.Screen
          name="game-betting"
          options={{
            title: "Game Bets",
            tabBarIcon: ({ color }) => (
              <Ionicons
                name="analytics"
                size={22}
                color={color}
              />
            ),
          }}
        />

        <Tabs.Screen
          name="live"
          options={{
            title: "Live",
            tabBarIcon: ({ color }) => (
              <Ionicons
                name="radio-outline"
                size={22}
                color={color}
              />
            ),
          }}
        />

        <Tabs.Screen
          name="first-basket"
          options={{
            title: "First Basket",
            tabBarIcon: ({ color }) => (
              <Ionicons
                name="basketball"
                size={22}
                color={color}
              />
            ),
          }}
        />

        <Tabs.Screen
          name="injuries"
          options={{
            title: "Injuries",
            tabBarIcon: ({ color }) => (
              <Ionicons
                name="medkit"
                size={22}
                color={color}
              />
            ),
          }}
        />

        <Tabs.Screen
          name="wowy"
          options={{
            title: "WOWY",
            tabBarIcon: ({ color }) => (
              <Ionicons
                name="people"
                size={22}
                color={color}
              />
            ),
          }}
        />

        {/* Ladders moved to home screen as a link */}
        <Tabs.Screen
          name="ladders"
          options={{
            href: null, // Hide from tab bar
          }}
        />

        {/* NBA Home (sport selector landing) */}
        <Tabs.Screen
          name="nba"
          options={{
            href: null, // Hide from tab bar
          }}
        />

        {/* PGA Home (sport selector landing) */}
        <Tabs.Screen
          name="pga"
          options={{
            href: null, // Hide from tab bar
          }}
        />

        {/* Bad Lines moved to home screen as a link */}
        <Tabs.Screen
          name="bad-lines"
          options={{
            href: null, // Hide from tab bar
          }}
        />

        {/* Live Props Dev - accessible from home */}
        <Tabs.Screen
          name="live-props-dev"
          options={{
            href: null, // Hide from tab bar
          }}
        />

        <Tabs.Screen
          name="trend-chart"
          options={{
            title: "Trends",
            tabBarIcon: ({ color }) => (
              <Ionicons
                name="trending-up"
                size={22}
                color={color}
              />
            ),
          }}
        />
      </Tabs>
    </>
  );
}