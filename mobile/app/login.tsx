// /app/login.ts
import { View, Pressable, Text } from "react-native";
import { login } from "@/lib/auth/login";

export default function LoginScreen() {
  return (
    <View
      style={{
        flex: 1,
        justifyContent: "center",
        alignItems: "center",
      }}
    >
      <Pressable
        onPress={login}
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