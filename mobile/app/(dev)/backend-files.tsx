import { useEffect, useState } from "react";
import { View, Text, Pressable, ScrollView } from "react-native";
import { useRouter } from "expo-router";
import { fetchBackendFiles } from "@/lib/api";

export default function BackendFilesScreen() {
  const [files, setFiles] = useState<string[]>([]);
  const router = useRouter();

  useEffect(() => {
    fetchBackendFiles().then(setFiles);
  }, []);

  return (
    <ScrollView style={{ padding: 16 }}>
      <Text style={{ fontSize: 20, fontWeight: "600", marginBottom: 12 }}>
        Backend Files
      </Text>

      {files.map((file) => (
        <Pressable
          key={file}
          onPress={() =>
            router.push({
              pathname: "/dev/code-viewer",
              params: { file },
            })
          }
          style={{
            paddingVertical: 12,
            borderBottomWidth: 1,
            borderBottomColor: "#eee",
          }}
        >
          <Text style={{ fontSize: 16 }}>{file}</Text>
        </Pressable>
      ))}
    </ScrollView>
  );
}