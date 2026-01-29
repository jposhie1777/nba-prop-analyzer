import { Stack } from "expo-router";
import { RouterDebug } from "@/lib/dev/RouterDebug";

export default function RootLayout() {
  console.log("🟥 ROOT LAYOUT RENDER");

  return (
    <>
      <Stack screenOptions={{ headerShown: false }} />
      {__DEV__ && <RouterDebug />}
    </>
  );
}