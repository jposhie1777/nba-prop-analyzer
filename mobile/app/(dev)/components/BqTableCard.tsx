import React from "react";
import { View, Text, Pressable } from "react-native";
import { useTheme } from "@/store/useTheme";
import { createDevStyles } from "../devStyles";
import { useBqTablePreview } from "@/lib/dev/useBqTablePreview";

type Props = {
  dataset: string;
  table: string;
};

export function BqTableCard({ dataset, table }: Props) {
  const { colors } = useTheme();
  const styles = React.useMemo(() => createDevStyles(colors), [colors]);

  const [open, setOpen] = React.useState(false);
  const preview = useBqTablePreview();

  async function toggle() {
    if (!open && !preview.columns) {
      await preview.load(dataset, table);
    }
    setOpen(!open);
  }

  return (
    <View style={styles.card}>
      <Pressable onPress={toggle}>
        <Text style={styles.cardTitle}>{table}</Text>
        <Text style={styles.mutedText}>
          {open ? "Tap to collapse" : "Tap to inspect"}
        </Text>
      </Pressable>

      {open && (
        <View style={{ marginTop: 8 }}>
          {preview.loading && (
            <Text style={styles.mutedText}>Loading…</Text>
          )}

          {preview.error && (
            <Text style={styles.dangerText}>{preview.error}</Text>
          )}

          {preview.columns && (
            <>
              <Text style={styles.cardSubtitle}>Columns</Text>
              {preview.columns.map((c) => (
                <Text key={c.column_name} style={styles.mutedText}>
                  {c.column_name} — {c.data_type}
                </Text>
              ))}
            </>
          )}

          {preview.row && (
            <>
              <Text style={[styles.cardSubtitle, { marginTop: 8 }]}>
                Example Row
              </Text>
              <Text
                style={styles.codeBlock}
                numberOfLines={10}
              >
                {JSON.stringify(preview.row, null, 2)}
              </Text>
            </>
          )}
        </View>
      )}
    </View>
  );
}