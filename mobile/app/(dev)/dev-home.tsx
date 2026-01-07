// /app/(dev)/dev-home.tsx
import React from "react";
import { ScrollView, View, Text, Pressable } from "react-native";
import Constants from "expo-constants";
import { useRouter } from "expo-router";

import { useTheme } from "@/store/useTheme";
import { createDevStyles } from "./devStyles";

/* 🔴 NEW */
import { useDevStore } from "../../lib/dev/devStore";

export default function DevHomeScreen() {
  const { colors } = useTheme();
  const styles = React.useMemo(() => createDevStyles(colors), [colors]);
  const router = useRouter();

  /* 🔴 NEW */
  const { health, actions } = useDevStore();

  const appVersion =
    Constants.expoConfig?.version ??
    // @ts-ignore
    Constants.manifest?.version ??
    "unknown";

  const runtimeEnv =
    Constants.expoConfig?.extra?.RUNTIME_ENV ??
    // @ts-ignore
    Constants.manifest?.extra?.RUNTIME_ENV ??
    "unknown";

  const apiUrl =
    Constants.expoConfig?.extra?.API_URL ??
    // @ts-ignore
    Constants.manifest?.extra?.API_URL ??
    "unknown";

  return (
    <ScrollView style={styles.screen} contentContainerStyle={styles.content}>
      <Text style={styles.title}>Dev Console</Text>

      {/* ENVIRONMENT */}
      <Section title="Environment & Build" styles={styles}>
        <KV label="ENV" value={runtimeEnv} styles={styles} />
        <KV label="API_URL" value={apiUrl} styles={styles} />
        <KV label="APP_VERSION" value={appVersion} styles={styles} />
      </Section>

      {/* 🔴 NEW: API HEALTH */}
      <Section title="API Health" styles={styles}>
        <ToolButton
          label="Run All Health Checks"
          subtitle="Ping backend debug endpoints"
          onPress={actions.runAllHealthChecks}
          styles={styles}
        />

        {health.checks.map((check) => (
          <View key={check.key} style={styles.card}>
            <Text style={styles.cardTitle}>{check.label}</Text>

            <Text style={styles.mutedText}>
              {check.lastStatus
                ? `Status ${check.lastStatus} • ${check.lastMs}ms`
                : "Not checked yet"}
            </Text>

            {check.error && (
              <Text style={styles.dangerText}>{check.error}</Text>
            )}

            {check.lastOkTs && (
              <Text style={styles.mutedText}>
                Last OK:{" "}
                {new Date(check.lastOkTs).toLocaleTimeString()}
              </Text>
            )}

            <Pressable
              style={[styles.toolButton, { marginTop: 8 }]}
              onPress={() => actions.runHealthCheck(check.key)}
            >
              <Text style={styles.toolTitle}>Run Check</Text>
            </Pressable>
          </View>
        ))}
      </Section>

      {/* TOOLS */}
      <Section title="Developer Tools" styles={styles}>
        <ToolButton
          label="Backend Files"
          subtitle="Browse backend endpoints & docs"
          onPress={() => router.push("/(dev)/backend-files")}
          styles={styles}
        />
        <ToolButton
          label="Code Viewer"
          subtitle="Quick access to important source files"
          onPress={() => router.push("/(dev)/code-viewer")}
          styles={styles}
        />
      </Section>
    </ScrollView>
  );
}

/* ---------------------------------- */
/* Reusable local components           */
/* ---------------------------------- */

function Section({
  title,
  children,
  styles,
}: {
  title: string;
  children: React.ReactNode;
  styles: any;
}) {
  const [open, setOpen] = React.useState(true);

  return (
    <View style={styles.section}>
      <Pressable style={styles.sectionHeader} onPress={() => setOpen(!open)}>
        <Text style={styles.sectionTitle}>{title}</Text>
        <Text style={styles.sectionChevron}>{open ? "▾" : "▸"}</Text>
      </Pressable>

      {open && <View style={styles.sectionBody}>{children}</View>}
    </View>
  );
}

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
      <Text style={styles.valueText} numberOfLines={2}>
        {value}
      </Text>
    </View>
  );
}

function ToolButton({
  label,
  subtitle,
  onPress,
  styles,
}: {
  label: string;
  subtitle?: string;
  onPress: () => void;
  styles: any;
}) {
  return (
    <Pressable style={styles.toolButton} onPress={onPress}>
      <Text style={styles.toolTitle}>{label}</Text>
      {subtitle && <Text style={styles.toolSubtitle}>{subtitle}</Text>}
    </Pressable>
  );
}