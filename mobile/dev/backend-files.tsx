import { useEffect, useState } from "react";
import { Text, Pressable, ScrollView } from "react-native";
import { useRouter } from "expo-router";

import { fetchBackendFiles } from "@/lib/api";
import { useTheme } from "@/store/useTheme";

export default function BackendFilesScreen() {
  const [files, setFiles] = useState<string[]>([]);
  const router = useRouter();
  const { colors } = useTheme();

  useEffect(() => {
    fetchBackendFiles().then(setFiles);
  }, []);

  return (
    <ScrollView
      style={{
        flex: 1,
        backgroundColor: colors.surface.screen,
        padding: 16,
      }}
      contentContainerStyle={{ paddingBottom: 24 }}
    >
      {/* Header */}
      <Text
        style={{
          fontSize: 20,
          fontWeight: "600",
          marginBottom: 16,
          color: colors.text.primary,
        }}
      >
        Backend Files
      </Text>

      {/* File list */}
      {files.map((file) => (
        <Pressable
          key={file}
          onPress={() =>
            router.push({
              pathname: "/(dev)/code-viewer",
              params: { file },
            })
          }
          style={{
            paddingVertical: 14,
            paddingHorizontal: 6,
            borderBottomWidth: 1,
            borderBottomColor: colors.border.subtle,
          }}
        >
          <Text
            style={{
              fontSize: 16,
              color: colors.text.primary,
            }}
          >
            {file}
          </Text>
        </Pressable>
      ))}

      {/* Empty state */}
      {files.length === 0 && (
        <Text
          style={{
            marginTop: 24,
            fontSize: 14,
            color: colors.text.muted,
          }}
        >
          No backend files found.
        </Text>
      )}
    </ScrollView>
  );
}
