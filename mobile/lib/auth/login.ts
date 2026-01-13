// lib/auth/login.ts
import * as AuthSession from "expo-auth-session";
import * as WebBrowser from "expo-web-browser";

WebBrowser.maybeCompleteAuthSession();

export async function login() {
  const domain = process.env.EXPO_PUBLIC_AUTH0_DOMAIN;
  const clientId = process.env.EXPO_PUBLIC_AUTH0_CLIENT_ID;

  if (!domain || !clientId) {
    throw new Error("Missing Auth0 environment variables");
  }

  const redirectUri = AuthSession.makeRedirectUri({
    useProxy: true,
  });

  const authUrl =
    `https://${domain}/authorize?` +
    `response_type=token` +
    `&client_id=${clientId}` +
    `&redirect_uri=${encodeURIComponent(redirectUri)}` +
    `&scope=openid profile email`;

  const result = await AuthSession.openAuthSessionAsync(
    authUrl,
    redirectUri
  );

  if (result.type !== "success") {
    throw new Error("Authentication cancelled");
  }

  // access_token is returned in the URL fragment
  const params = new URLSearchParams(
    result.url.split("#")[1]
  );

  const accessToken = params.get("access_token");

  if (!accessToken) {
    throw new Error("No access token returned");
  }

  return accessToken;
}