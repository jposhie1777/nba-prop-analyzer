// /app/(dev)/network-logs.tsx
import React from "react";
import { ScrollView, View, Text, Pressable } from "react-native";
import { useRouter } from "expo-router";

import { useTheme } from "@/store/useTheme";
import { createDevStyles } from "./devStyles";
import { useDevStore } from "@/lib/dev/devStore";

export default function NetworkLogsScreen() {
  const { colors } = useTheme();
  const styles = React.useMemo(() => createDevStyles(colors), [colors]);
  const router = useRouter();

  const { network, actions } = useDevStore();
  const [selectedId, setSelectedId] = React.useState<string | null>(null);

  const selected = network.items.find((n) => n.id === selectedId) ?? null;

  return (
    <ScrollView style={styles.screen} contentContainerStyle={styles.content}>
      <Text style={styles.title}>Network Logs</Text>

      {/* HEADER ACTIONS */}
      <View style={styles.rowButtons}>
        <Pressable style={styles.toolButton} onPress={actions.clearNetwork}>
          <Text style={styles.toolTitle}>Clear</Text>
        </Pressable>

        <Pressable
          style={styles.toolButton}
          onPress={() => actions.copyDevReport("network")}
        >
          <Text style={styles.toolTitle}>Copy JSON</Text>
        </Pressable>

        <Pressable
          style={styles.toolButton}
          onPress={() => router.back()}
        >
          <Text style={styles.toolTitle}>Back</Text>
        </Pressable>
      </View>

      {/* LIST */}
      {network.items.length === 0 && (
        <Text style={styles.mutedText}>No network activity yet</Text>
      )}

      {network.items.map((n) => (
        <Pressable
          key={n.id}
          style={styles.card}
          onPress={() =>
            setSelectedId((prev) => (prev === n.id ? null : n.id))
          }
        >
          <Text style={styles.cardTitle}>
            {n.method}{" "}
            {n.status ? n.status : "—"} •{" "}
            {n.ms ? `${n.ms}ms` : "—"}
          </Text>

          <Text style={styles.mono} numberOfLines={2}>
            {n.url}
          </Text>

          {n.error && (
            <Text style={styles.dangerText} numberOfLines={2}>
              {n.error}
            </Text>
          )}

          {/* DETAILS */}
          {selectedId === n.id && (
            <View style={{ marginTop: 8, gap: 6 }}>
              <KV label="Method" value={n.method} styles={styles} />
              <KV label="URL" value={n.url} styles={styles} />
              {n.status !== undefined && (
                <KV
                  label="Status"
                  value={String(n.status)}
                  styles={styles}
                />
              )}
              {n.ms !== undefined && (
                <KV
                  label="Duration"
                  value={`${n.ms} ms`}
                  styles={styles}
                />
              )}
              <KV
                label="Timestamp"
                value={new Date(n.ts).toLocaleString()}
                styles={styles}
              />

              <Pressable
                style={[styles.toolButton, { marginTop: 6 }]}
                onPress={() => copyAsCurl(n)}
              >
                <Text style={styles.toolTitle}>Copy as curl</Text>
              </Pressable>
            </View>
          )}
        </Pressable>
      ))}
    </ScrollView>
  );
}

/* ---------------------------------- */
/* Helpers                            */
/* ---------------------------------- */

function KV({
  label,
  value,
  styles,
}: {
  label: string;
  value: string;
  styles: any;
}) {
  return (
    <View style={styles.kvRow}>
      <Text style={styles.keyText}>{label}</Text>
      <Text style={styles.valueText} numberOfLines={3}>
        {value}
      </Text>
    </View>
  );
}

function copyAsCurl(n: {
  method: string;
  url: string;
}) {
  const curl = `curl -X ${n.method} "${n.url}"`;
  // We reuse the dev store clipboard helper indirectly
  // but keep this local & lightweight
  // eslint-disable-next-line no-console
  console.log(curl);
}