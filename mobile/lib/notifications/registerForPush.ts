// lib/notifications/registerForPush.ts
import * as Notifications from "expo-notifications";
import * as Device from "expo-device";
import Constants from "expo-constants";
import AsyncStorage from "@react-native-async-storage/async-storage";
import { API_BASE } from "@/lib/config";

const TOKEN_CACHE_KEY = "expo_push_token_last";

/**
 * Registers the device for Expo push notifications.
 * Safe to call on every app launch — registration is token-change gated.
 */
export async function ensurePushRegistered(userId: string) {
  try {
    // ❌ Simulators do not support push
    if (!Device.isDevice) {
      console.log("🧪 Simulator detected — skipping push registration");
      return;
    }

    // 🔐 Permission check / request (once)
    const perm = await Notifications.getPermissionsAsync();
    let finalStatus = perm.status;

    if (perm.status === "undetermined") {
      const req = await Notifications.requestPermissionsAsync();
      finalStatus = req.status;
    }

    if (finalStatus !== "granted") {
      console.log("🔕 Push permission not granted");
      return;
    }

    // 🔑 Required for stable Expo push tokens
    const projectId =
      Constants.expoConfig?.extra?.eas?.projectId ??
      Constants.easConfig?.projectId;

    if (!projectId) {
      console.warn("⚠️ Missing EAS projectId — cannot fetch push token");
      return;
    }

    // 📲 Fetch token
    const token = (
      await Notifications.getExpoPushTokenAsync({ projectId })
    ).data;

    // 🧠 Skip if unchanged
    const lastToken = await AsyncStorage.getItem(TOKEN_CACHE_KEY);
    if (lastToken === token) {
      console.log("✅ Push token unchanged — skipping backend register");
      return;
    }

    console.log("📲 New push token detected — registering", token);

    // 📡 Register with backend
    const res = await fetch(`${API_BASE}/push/register`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        user_id: userId,
        expo_push_token: token,
      }),
    });

    if (!res.ok) {
      const text = await res.text();
      console.warn(
        "❌ Push registration failed:",
        res.status,
        text
      );
      return;
    }

    // 💾 Persist token
    await AsyncStorage.setItem(TOKEN_CACHE_KEY, token);
    console.log("✅ Push registration successful");
  } catch (err) {
    console.warn("❌ ensurePushRegistered error", err);
  }
}