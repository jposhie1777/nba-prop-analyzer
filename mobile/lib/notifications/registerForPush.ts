// lib/notifications/registerForPush.ts
import * as Notifications from "expo-notifications";
import * as Device from "expo-device";
import Constants from "expo-constants";
import AsyncStorage from "@react-native-async-storage/async-storage";
import { API_BASE } from "@/lib/apiMaster";

const STORAGE_KEY = "expo_push_token_last";

/**
 * Registers the device for push notifications
 * - Safe on Expo Go
 * - No-op on simulators
 * - Only POSTs when token changes
 */
export async function ensurePushRegistered(userId: string) {
  try {
    // ❌ Simulators cannot receive push
    if (!Device.isDevice) {
      console.log("📵 Push skipped (not a physical device)");
      return;
    }

    const perms = await Notifications.getPermissionsAsync();
    if (perms.status !== "granted") {
      console.log("📵 Push permission not granted");
      return;
    }

    const projectId =
      Constants.expoConfig?.extra?.eas?.projectId ??
      Constants.easConfig?.projectId;

    if (!projectId) {
      console.warn("⚠️ Missing EAS projectId — cannot fetch stable push token");
      return;
    }

    const token = (
      await Notifications.getExpoPushTokenAsync({ projectId })
    ).data;

    const lastToken = await AsyncStorage.getItem(STORAGE_KEY);

    if (lastToken === token) {
      console.log("✅ Push token unchanged — skipping backend register");
      return;
    }

    console.log("📲 Registering new Expo push token");

    await fetch(`${API_BASE}/push/register`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        user_id: userId,
        expo_push_token: token,
      }),
    });

    await AsyncStorage.setItem(STORAGE_KEY, token);
  } catch (err) {
    // ❗ Never crash app boot because of push
    console.warn("📵 Push registration failed:", err);
  }
}