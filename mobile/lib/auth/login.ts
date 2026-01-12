// lib/auth/login.ts
import * as AuthSession from "expo-auth-session";
import { useAuth } from "./useAuth";

export async function login() {
  const redirectUri = AuthSession.makeRedirectUri({ useProxy: true });

  const authUrl =
    `https://${process.env.EXPO_PUBLIC_AUTH0_DOMAIN}/authorize` +
    `?client_id=${process.env.EXPO_PUBLIC_AUTH0_CLIENT_ID}` +
    `&response_type=token` +
    `&scope=openid profile email` +
    `&audience=https://api.pulse.app` +
    `&redirect_uri=${encodeURIComponent(redirectUri)}`;

  const result = await AuthSession.startAsync({ authUrl });

  if (result.type === "success") {
    const { access_token } = result.params as any;

    // decode role from JWT payload
    const payload = JSON.parse(
      atob(access_token.split(".")[1])
    );

    useAuth.getState().setAuth(
      access_token,
      payload.role
    );
  }
}
