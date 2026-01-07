import { useEffect, useState } from "react";
import { ScrollView, Text, View, Pressable } from "react-native";
import { useLocalSearchParams, useRouter } from "expo-router";

import { fetchBackendFile } from "@/lib/api";
import { useTheme } from "@/store/useTheme";

export default function CodeViewerScreen() {
  const { file } = useLocalSearchParams<{ file: string }>();
  const router = useRouter();
  const { colors } = useTheme();

  const [code, setCode] = useState<string>("");

  useEffect(() => {
    if (file) {
      fetchBackendFile(file).then(setCode);
    }
  }, [file]);

  return (
    <View
      style={{
        flex: 1,
        backgroundColor: colors.surface.screen,
      }}
    >
      {/* Header */}
      <View
        style={{
          flexDirection: "row",
          alignItems: "center",
          paddingHorizontal: 12,
          paddingVertical: 10,
          borderBottomWidth: 1,
          borderBottomColor: colors.border.subtle,
        }}
      >
        <Pressable onPress={() => router.back()}>
          <Text
            style={{
              fontSize: 18,
              marginRight: 12,
              color: colors.accent.primary,
            }}
          >
            ←
          </Text>
        </Pressable>

        <Text
          numberOfLines={1}
          style={{
            flex: 1,
            fontSize: 16,
            fontWeight: "600",
            color: colors.text.primary,
          }}
        >
          {file}
        </Text>
      </View>

      {/* Code */}
      <ScrollView
        style={{ flex: 1 }}
        contentContainerStyle={{
          padding: 16,
          paddingBottom: 40,
        }}
      >
        <Text
          selectable
          style={{
            fontFamily: "monospace",
            fontSize: 12,
            lineHeight: 18,
            color: colors.text.primary,
          }}
        >
          {code || "// Loading…"}
        </Text>
      </ScrollView>
    </View>
  );
}