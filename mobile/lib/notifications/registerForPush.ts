// lib/notifications/registerForPush.ts
import * as Notifications from "expo-notifications";
import * as Device from "expo-device";
import Constants from "expo-constants";
import AsyncStorage from "@react-native-async-storage/async-storage";
import { API_BASE } from "@/lib/apiMaster";

const STORAGE_KEY = "expo_push_token_last";
const FETCH_TIMEOUT_MS = 8000;

export async function ensurePushRegistered(userId: string) {
  console.log("🔔 ensurePushRegistered START");

  try {
    /* ---------------------------------
       DEVICE CHECK
    ---------------------------------- */
    if (!Device.isDevice) {
      console.log("❌ Not a physical device");
      return;
    }

    /* ---------------------------------
       PERMISSIONS
    ---------------------------------- */
    const perms = await Notifications.getPermissionsAsync();
    console.log("🔐 Push permissions:", perms);

    if (perms.status !== "granted") {
      console.log("❌ Push permission not granted");
      return;
    }

    /* ---------------------------------
       EAS PROJECT ID
    ---------------------------------- */
    const projectId =
      Constants.expoConfig?.extra?.eas?.projectId ??
      Constants.easConfig?.projectId;

    console.log("🆔 EAS projectId:", projectId);

    if (!projectId) {
      console.log("❌ Missing projectId — token fetch aborted");
      return;
    }

    /* ---------------------------------
       FETCH EXPO PUSH TOKEN
    ---------------------------------- */
    const tokenResult = await Notifications.getExpoPushTokenAsync({
      projectId,
    });

    console.log("📲 Raw token result:", tokenResult);

    const token = tokenResult?.data;

    if (!token) {
      console.log("❌ Token fetch returned null");
      return;
    }

    console.log("📲 Expo push token:", token);

    /* ---------------------------------
       CACHE CHECK
    ---------------------------------- */
    const lastToken = await AsyncStorage.getItem(STORAGE_KEY);
    console.log("🧠 Cached token:", lastToken);

    if (lastToken === token) {
      console.log("⚠️ Token unchanged — skipping backend register");
      return;
    }

    /* ---------------------------------
       API BASE VALIDATION (CRITICAL)
    ---------------------------------- */
    console.log("🌐 API_BASE:", API_BASE);

    if (!API_BASE) {
      console.log("❌ API_BASE is undefined — aborting push register");
      return;

 }

    /* ---------------------------------
       REGISTER WITH BACKEND
    ---------------------------------- */
    console.log("🚀 Registering token with backend…");

    const controller = new AbortController();
    const timeout = setTimeout(
      () => controller.abort(),
      FETCH_TIMEOUT_MS
    );

    let resp: Response;

    try {
      resp = await fetch(`${API_BASE}/push/register`, {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
          Accept: "application/json",
        },
        body: JSON.stringify({
          user_id: userId,
          expo_push_token: token,
        }),
        signal: controller.signal,
      });
    } finally {
      clearTimeout(timeout);
    }

    console.log("📡 Backend response status:", resp.status);

    if (!resp.ok) {
      const text = await resp.text();
      console.log("❌ Backend error response:", text);
      return;
    }

    await AsyncStorage.setItem(STORAGE_KEY, token);
    console.log("✅ Push token registered + cached");
  } catch (err) {
    console.log("🔥 ensurePushRegistered crashed:", err);
  }
}
