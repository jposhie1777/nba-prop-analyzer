// app/login.tsx
import { View, Text, Pressable, StyleSheet } from "react-native";
import { login } from "@/lib/auth/login";

export default function LoginScreen() {
  return (
    <View style={styles.container}>
      <Text style={styles.title}>Pulse</Text>

      <Pressable style={styles.button} onPress={login}>
        <Text style={styles.buttonText}>Sign in</Text>
      </Pressable>
    </View>
  );
}

const styles = StyleSheet.create({
  container: {
    flex: 1,
    justifyContent: "center",
    alignItems: "center",
  },
  title: {
    fontSize: 28,
    marginBottom: 24,
    fontWeight: "600",
  },
  button: {
    backgroundColor: "#000",
    paddingVertical: 14,
    paddingHorizontal: 28,
    borderRadius: 8,
  },
  buttonText: {
    color: "#fff",
    fontSize: 16,
  },
});