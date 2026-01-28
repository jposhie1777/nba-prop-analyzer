// lib/notifications/registerForPush.ts
import * as Notifications from "expo-notifications";
import * as Device from "expo-device";
import Constants from "expo-constants";
import AsyncStorage from "@react-native-async-storage/async-storage";
import { API_BASE } from "@/lib/config";

const KEY = "expo_push_token_last";

export async function ensurePushRegistered(userId: string) {
  if (!Device.isDevice) return; // simulators don't get push

  const perm = await Notifications.getPermissionsAsync();
  if (perm.status !== "granted") return;

  const projectId =
    Constants.expoConfig?.extra?.eas?.projectId ??
    Constants.easConfig?.projectId;

  if (!projectId) {
    console.log("⚠️ Missing EAS projectId (needed for stable token fetch)");
    return;
  }

  const token = (await Notifications.getExpoPushTokenAsync({ projectId })).data;
  const last = await AsyncStorage.getItem(KEY);

  if (last === token) {
    console.log("✅ Push token unchanged; skipping register");
    return;
  }

  console.log("📲 New push token; registering…", token);
  await fetch(`${API_BASE}/push/register`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ user_id: userId, expo_push_token: token }),
  });

  await AsyncStorage.setItem(KEY, token);
}