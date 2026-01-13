import * as WebBrowser from "expo-web-browser";
import * as AuthSession from "expo-auth-session";
import Constants from "expo-constants";

WebBrowser.maybeCompleteAuthSession();

export async function login() {
  
  const domain = Constants.expoConfig?.extra?.AUTH0_DOMAIN;
  const clientId = Constants.expoConfig?.extra?.AUTH0_CLIENT_ID;

  if (!domain || !clientId) {
    throw new Error("Missing Auth0 environment variables");
  }

  const redirectUri = AuthSession.makeRedirectUri({
    useProxy: true,
  });

  const authUrl =
    `https://${domain}/authorize` +
    `?response_type=token` +
    `&client_id=${clientId}` +
    `&redirect_uri=${encodeURIComponent(redirectUri)}` +
    `&scope=openid profile email`;

  const result = await WebBrowser.openAuthSessionAsync(
    authUrl,
    redirectUri
  );

  if (result.type !== "success") {
    throw new Error("Login cancelled");
  }

  const params = AuthSession.getQueryParams(result.url);
  const accessToken = params.access_token;

  if (!accessToken) {
    throw new Error("No access token returned");
  }

  return accessToken;
}