// lib/notifications/registerForPush.ts
import * as Notifications from "expo-notifications";
import * as Device from "expo-device";

export async function registerForPushNotifications() {
  if (!Device.isDevice) return null;

  const { status } = await Notifications.getPermissionsAsync();
  let finalStatus = status;

  if (status !== "granted") {
    const req = await Notifications.requestPermissionsAsync();
    finalStatus = req.status;
  }

  if (finalStatus !== "granted") return null;

  const token = (await Notifications.getExpoPushTokenAsync()).data;
  return token;
}