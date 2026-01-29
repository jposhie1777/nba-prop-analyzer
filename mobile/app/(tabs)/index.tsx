// app/(tabs)/index.tsx
import { Redirect } from "expo-router";

export default function TabsIndex() {
  return <Redirect href="/(tabs)/home" />;
}