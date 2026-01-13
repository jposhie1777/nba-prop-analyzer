// app/login.tsx
import { View, Text, Pressable, ActivityIndicator } from "react-native";
import { useAuth } from "@/lib/auth/useAuth";
import { login } from "@/lib/auth/login";
import { Redirect } from "expo-router";

export default function LoginScreen() {
  const accessToken = useAuth((s) => s.accessToken);
  const loading = useAuth((s) => s.loading);

  if (accessToken) {
    return <Redirect href="/" />;
  }

  return (
    <View style={{ flex: 1, justifyContent: "center", alignItems: "center" }}>
      {loading ? (
        <ActivityIndicator size="large" />
      ) : (
        <Pressable
          onPress={login}
          style={{
            padding: 16,
            backgroundColor: "#4f46e5",
            borderRadius: 10,
          }}
        >
          <Text style={{ color: "white", fontSize: 16 }}>
            Sign in with Auth0
          </Text>
        </Pressable>
      )}
    </View>
  );
}