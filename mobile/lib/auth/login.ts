import * as AuthSession from "expo-auth-session";
import { useAuth } from "./useAuth";

export async function login() {
  const domain = process.env.EXPO_PUBLIC_AUTH0_DOMAIN;
  const clientId = process.env.EXPO_PUBLIC_AUTH0_CLIENT_ID;

  if (!domain || !clientId) {
    throw new Error("Missing Auth0 environment variables");
  }

  // ✅ Let Expo generate the correct redirect (works with tunnel, LAN, Go)
  const redirectUri = AuthSession.makeRedirectUri({
    useProxy: true,
  });

  const authUrl =
    `https://${domain}/authorize` +
    `?client_id=${clientId}` +
    `&response_type=token` +
    `&scope=openid profile email` +
    `&redirect_uri=${encodeURIComponent(redirectUri)}`;

  const result = await AuthSession.startAsync({
    authUrl,
  });

  if (result.type !== "success") {
    throw new Error("Login cancelled");
  }

  const accessToken = result.params?.access_token;

  if (!accessToken) {
    throw new Error("No access token returned");
  }

  // Optional: store globally
  useAuth.getState().setAccessToken(accessToken);

  return accessToken;
}