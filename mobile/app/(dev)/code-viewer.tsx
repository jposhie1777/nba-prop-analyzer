import { useEffect, useState } from "react";
import { ScrollView, Text } from "react-native";
import { useLocalSearchParams } from "expo-router";
import { fetchBackendFile } from "@/lib/api";

export default function CodeViewerScreen() {
  const { file } = useLocalSearchParams<{ file: string }>();
  const [code, setCode] = useState("");

  useEffect(() => {
    if (file) {
      fetchBackendFile(file).then(setCode);
    }
  }, [file]);

  return (
    <ScrollView
      style={{ padding: 12 }}
      contentContainerStyle={{ paddingBottom: 40 }}
    >
      <Text
        selectable
        style={{
          fontFamily: "monospace",
          fontSize: 12,
          lineHeight: 18,
        }}
      >
        {code}
      </Text>
    </ScrollView>
  );
}