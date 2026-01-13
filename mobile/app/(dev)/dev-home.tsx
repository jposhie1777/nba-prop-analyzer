// /app/(dev)/dev-home.tsx
import React from "react";
import { ScrollView, View, Text, Pressable, AppState } from "react-native";
import Constants from "expo-constants";
import { useRouter } from "expo-router";

import { useTheme } from "@/store/useTheme";
import { createDevStyles } from "./devStyles";
import { useDevStore } from "@/lib/dev/devStore";
import { BqTableCard } from "./components/BqTableCard";

export default function DevHomeScreen() {
  const { colors } = useTheme();
  const styles = React.useMemo(() => createDevStyles(colors), [colors]);
  const router = useRouter();

  const { health, flags, sse, freshness, actions } = useDevStore();

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

  /* --------------------------------------------------
     🔴 4D: AUTO-REFRESH ON APP RESUME (DEV ONLY)
-------------------------------------------------- */
  React.useEffect(() => {
    if (!__DEV__) return;

    const sub = AppState.addEventListener("change", (state) => {
      if (state === "active") {
        actions.runAllHealthChecks();
        freshness.datasets.forEach((d) =>
          actions.fetchFreshness(d.key)
        );
      }
    });

    return () => {
      sub.remove();
    };
  }, [actions, freshness.datasets]);

  /* 🔴 ALSO: run once on initial mount */
  React.useEffect(() => {
    if (!__DEV__) return;

    actions.runAllHealthChecks();
    freshness.datasets.forEach((d) =>
      actions.fetchFreshness(d.key)
    );
  }, []);

  return (
    <ScrollView style={styles.screen} contentContainerStyle={styles.content}>
      <Text style={styles.title}>Dev Console</Text>

      {/* ENVIRONMENT */}
      <Section title="Environment & Build" styles={styles}>
        <KV label="ENV" value={runtimeEnv} styles={styles} />
        <KV label="API_URL" value={apiUrl} styles={styles} />
        <KV label="APP_VERSION" value={appVersion} styles={styles} />
      </Section>

      {/* API HEALTH */}
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
                Last OK: {new Date(check.lastOkTs).toLocaleTimeString()}
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

      {/* FEATURE FLAGS */}
      <Section title="Feature Flags" styles={styles}>
        {Object.entries(flags.values).map(([key, enabled]) => (
          <Pressable
            key={key}
            style={[
              styles.card,
              enabled && { borderColor: colors.accent.primary },
            ]}
            onPress={() => actions.toggleFlag(key)}
          >
            <View
              style={{ flexDirection: "row", justifyContent: "space-between" }}
            >
              <Text style={styles.cardTitle}>{key}</Text>
              <Text
                style={[
                  styles.cardTitle,
                  enabled ? styles.on : styles.mutedText,
                ]}
              >
                {enabled ? "ON" : "OFF"}
              </Text>
            </View>

            <Text style={styles.mutedText}>Tap to toggle (local only)</Text>
          </Pressable>
        ))}
      </Section>

      {/* LIVE STREAM (SSE) */}
      <Section title="Live Stream (SSE)" styles={styles}>
        <View style={styles.card}>
          <Text style={styles.cardTitle}>
            Status:{" "}
            <Text
              style={{
                color: sse.connected
                  ? colors.accent.success
                  : colors.accent.danger,
              }}
            >
              {sse.connected ? "CONNECTED" : "DISCONNECTED"}
            </Text>
          </Text>

          <Text style={styles.mutedText}>Events received: {sse.eventCount}</Text>

          {sse.lastEventTs && (
            <Text style={styles.mutedText}>
              Last event: {new Date(sse.lastEventTs).toLocaleTimeString()}
            </Text>
          )}

          {sse.lastError && (
            <Text style={styles.dangerText}>Error: {sse.lastError}</Text>
          )}
        </View>

        <Text style={styles.mutedText}>
          Reflects the current live score stream connection state.
        </Text>
      </Section>

      {/* DATA FRESHNESS */}
      <Section title="Data Freshness" styles={styles}>
        {freshness.datasets.map((d) => (
          <View key={d.key} style={styles.card}>
            ...
          </View>
        ))}
      
        <Text style={styles.mutedText}>
          Confirms backend ingestion & BigQuery freshness.
        </Text>
      </Section>
      
      {/* BIGQUERY TABLE INSPECTOR */}
      <Section
        title="BigQuery Tables"
        styles={styles}
        defaultOpen={false}
      >
        <Text style={styles.mutedText}>
          Schema + latest row (dev only, safe preview)
        </Text>
 
        {[
          "props_mobile_v1",
          "props_full_enriched",
          "live_games",
          "live_player_stats",
          "player_game_stats",
          "player_advanced_rollups",
          "historical_player_trends",
        ].map((table) => (
          <BqTableCard
            key={table}
            dataset="nba_goat_data"
            table={table}
          />
        ))}
      </Section>

      {/* DEVELOPER TOOLS */}
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

        <ToolButton
          label="Network Logs"
          subtitle="Inspect API calls, latency, and errors"
          onPress={() => router.push("/(dev)/network-logs")}
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
  defaultOpen = true,
}: {
  title: string;
  children: React.ReactNode;
  styles: any;
  defaultOpen?: boolean;
}) {
  const [open, setOpen] = React.useState(defaultOpen);

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
