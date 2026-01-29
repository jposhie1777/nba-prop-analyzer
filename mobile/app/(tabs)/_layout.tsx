// app/(tabs)/_layout.tsx
import { Tabs } from "expo-router";
import { Ionicons } from "@expo/vector-icons";
import { Platform } from "react-native";

import { HapticTab } from "@/components/haptic-tab";
import { Colors } from "@/constants/theme";
import { useColorScheme } from "@/hooks/use-color-scheme";
import { PulseHeader } from "@/components/layout/PulseHeader";
import { LiveDataBridge } from "@/components/LiveDataBridge";

export default function TabLayout() {
  const colorScheme = useColorScheme();

  return (
    <>
      {/* 🔥 Live data sync — NATIVE ONLY */}
      {Platform.OS !== "web" && <LiveDataBridge />}

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