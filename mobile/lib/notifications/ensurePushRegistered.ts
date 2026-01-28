import AsyncStorage from "@react-native-async-storage/async-storage";
import { registerForPushNotifications } from "./registerForPush";
import { API_BASE } from "@/lib/apiMaster";

const STORAGE_KEY = "last_expo_push_token";

export async function ensurePushRegistered(userId: string) {
  const token = await registerForPushNotifications();
  if (!token) return;

  const lastToken = await AsyncStorage.getItem(STORAGE_KEY);

  // ✅ Skip if unchanged
  if (lastToken === token) {
    console.log("📵 Push token unchanged, skipping registration");
    return;
  }

  console.log("📲 Registering new push token");

  await fetch(`${API_BASE}/push/register`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({
      user_id: userId,
      expo_push_token: token,
    }),
  });

  await AsyncStorage.setItem(STORAGE_KEY, token);
}