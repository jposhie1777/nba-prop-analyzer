import { View, Pressable, Text } from "react-native";
import { login } from "@/lib/auth/login";
import { useAuth } from "@/lib/auth/useAuth";

export default function LoginScreen() {
  const setAuth = useAuth((s) => s.setAuth);

  const handleLogin = async () => {
    try {
      const token = await login();

      // TEMP: force dev role
      setAuth(token, "dev");
    } catch (err) {
      console.error("Login failed:", err);
    }
  };

  return (
    <View style={{ flex: 1, justifyContent: "center", alignItems: "center" }}>
      <Pressable
        onPress={handleLogin}
        style={{
          paddingHorizontal: 24,
          paddingVertical: 14,
          borderRadius: 12,
          backgroundColor: "#6C5CE7",
        }}
      >
        <Text style={{ color: "white", fontSize: 16 }}>
          Sign in with Auth0
        </Text>
      </Pressable>
    </View>
  );
}