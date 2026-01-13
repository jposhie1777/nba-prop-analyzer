import * as AuthSession from "expo-auth-session";
import { useAuth } from "./useAuth";

// REQUIRED for Expo Go + Web
AuthSession.maybeCompleteAuthSession();

const domain = process.env.EXPO_PUBLIC_AUTH0_DOMAIN!;
const clientId = process.env.EXPO_PUBLIC_AUTH0_CLIENT_ID!;

export async function login() {
  // ✅ Correct redirect for Expo Go, iOS, Android, Web
  const redirectUri = AuthSession.makeRedirectUri({
    useProxy: true, // CRITICAL for Expo Go
  });

  const authUrl =
    `https://${domain}/authorize` +
    `?response_type=token` +
    `&client_id=${clientId}` +
    `&redirect_uri=${encodeURIComponent(redirectUri)}` +
    `&scope=openid profile email`;

  const result = await AuthSession.startAsync({
    authUrl,
  });

  if (result.type !== "success") return;

  const accessToken = (result.params as any)?.access_token;
  if (!accessToken) return;

  useAuth.getState().actions.setAccessToken(accessToken);
}