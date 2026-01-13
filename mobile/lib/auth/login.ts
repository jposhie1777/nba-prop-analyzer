import * as AuthSession from "expo-auth-session";
import * as WebBrowser from "expo-web-browser";
import Constants from "expo-constants";
import { Platform } from "react-native";
import { useAuth } from "./useAuth";

WebBrowser.maybeCompleteAuthSession();

const domain = process.env.EXPO_PUBLIC_AUTH0_DOMAIN!;
const clientId = process.env.EXPO_PUBLIC_AUTH0_CLIENT_ID!;

// ✅ SAFE redirect resolution for ALL platforms
const redirectUri = AuthSession.makeRedirectUri({
  scheme: "pulse",
  useProxy: Platform.OS !== "web",
});

export async function login() {
  const authUrl =
    `https://${domain}/authorize?` +
    new URLSearchParams({
      client_id: clientId,
      response_type: "token",
      scope: "openid profile email",
      redirect_uri: redirectUri,
    }).toString();

  const result = await WebBrowser.openAuthSessionAsync(
    authUrl,
    redirectUri
  );

  if (result.type !== "success") return;

  const params = AuthSession.parseRedirectUri(result.url);
  const accessToken = params.access_token;

  if (!accessToken) return;

  useAuth.getState().actions.setAccessToken(accessToken);
}