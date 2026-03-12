// /dev/dev-home.tsx  —  Dev Console (Workflow Triggers)
import React from "react";
import {
  ScrollView,
  View,
  Text,
  Pressable,
  TextInput,
  StyleSheet,
  AppState,
} from "react-native";
import { useRouter } from "expo-router";
import { useTheme } from "@/store/useTheme";
import { useDevStore } from "@/lib/dev/devStore";
import { createDevStyles } from "./devStyles";

/* ─────────────────────────────────────────────
   Section groupings for the trigger list
───────────────────────────────────────────── */
const DAILY_LOADER_IDS = [
  "epl_daily_loader.yml",
  "mls_daily.yml",
  "atp_daily_loader.yml",
  "atp_odds_daily.yml",
  "sheets_bq_sync.yml",
  "pga_daily_ingest.yml",
  "pga_odds_daily.yml",
  "soccer_odds_sheets_sync.yml",
];

const BACKFILL_IDS = [
  "epl_backfill.yml",
  "mls_backfill.yml",
  "atp_backfill.yml",
  "pga_backfill.yml",
];

/* ─────────────────────────────────────────────
   Main screen
───────────────────────────────────────────── */
export default function DevHomeScreen() {
  const { colors } = useTheme();
  const styles = React.useMemo(() => createDevStyles(colors), [colors]);
  const router = useRouter();

  const { devUnlocked, githubPat, workflows, actions } = useDevStore();

  /* Hydrate PAT on mount */
  React.useEffect(() => {
    actions.hydrateGithubPat();
  }, []);

  /* Reset unlock when screen is hidden */
  React.useEffect(() => {
    const sub = AppState.addEventListener("change", (state) => {
      if (state === "background") actions.resetDevUnlock();
    });
    return () => sub.remove();
  }, [actions]);

  /* ── Locked gate ── */
  if (!devUnlocked) {
    return (
      <ScrollView style={styles.screen} contentContainerStyle={styles.content}>
        <Text style={styles.title}>Dev Console</Text>
        <Text style={styles.mutedText}>
          Tap the Pulse logo 4 times to unlock.
        </Text>
        <Pressable style={styles.toolButton} onPress={() => router.back()}>
          <Text style={styles.toolTitle}>← Back</Text>
        </Pressable>
      </ScrollView>
    );
  }

  const triggers = workflows.triggers;
  const dailyTriggers = triggers.filter((t) => DAILY_LOADER_IDS.includes(t.id));
  const backfillTriggers = triggers.filter((t) => BACKFILL_IDS.includes(t.id));

  return (
    <ScrollView style={styles.screen} contentContainerStyle={styles.content}>
      {/* Header */}
      <View style={localStyles.headerRow}>
        <Text style={styles.title}>Dev Console</Text>
        <Pressable
          style={[styles.toolButton, localStyles.lockBtn]}
          onPress={() => {
            actions.resetDevUnlock();
            router.back();
          }}
        >
          <Text style={styles.toolTitle}>🔒 Lock</Text>
        </Pressable>
      </View>

      {/* ── GitHub Auth ── */}
      <Section title="GitHub Auth" styles={styles} defaultOpen>
        <PATInput
          pat={githubPat}
          colors={colors}
          styles={styles}
          onSave={async (val) => {
            await actions.setGithubPat(val);
          }}
          onClear={async () => {
            await actions.setGithubPat("");
          }}
        />
        <Text style={[styles.mutedText, { marginTop: 6 }]}>
          Needs a GitHub PAT with <Text style={{ fontStyle: "italic" }}>workflow</Text> scope.
          Stored locally on this device only.
        </Text>
      </Section>

      {/* ── Daily Loaders ── */}
      <Section title="Daily Loaders" styles={styles} defaultOpen>
        {dailyTriggers.map((t) => (
          <WorkflowRow
            key={t.id}
            trigger={t}
            styles={styles}
            colors={colors}
            onPress={() => actions.triggerWorkflow(t.id)}
          />
        ))}
      </Section>

      {/* ── Manual Backfills ── */}
      <Section title="Manual Backfills" styles={styles} defaultOpen={false}>
        <Text style={[styles.mutedText, { marginBottom: 8 }]}>
          Long-running — kicks off a GitHub Actions run immediately.
        </Text>
        {backfillTriggers.map((t) => (
          <WorkflowRow
            key={t.id}
            trigger={t}
            styles={styles}
            colors={colors}
            onPress={() => actions.triggerWorkflow(t.id)}
          />
        ))}
      </Section>
    </ScrollView>
  );
}

/* ─────────────────────────────────────────────
   PAT input with masked display
───────────────────────────────────────────── */
function PATInput({
  pat,
  colors,
  styles,
  onSave,
  onClear,
}: {
  pat: string;
  colors: any;
  styles: any;
  onSave: (val: string) => Promise<void>;
  onClear: () => Promise<void>;
}) {
  const [editing, setEditing] = React.useState(false);
  const [draft, setDraft] = React.useState("");

  const masked = pat
    ? `${pat.slice(0, 4)}${"•".repeat(Math.max(0, pat.length - 8))}${pat.slice(-4)}`
    : "(not set)";

  if (!editing) {
    return (
      <View style={localStyles.patRow}>
        <Text style={[styles.valueText, { flex: 1 }]}>{masked}</Text>
        <Pressable
          style={[styles.toolButton, localStyles.smallBtn]}
          onPress={() => {
            setDraft("");
            setEditing(true);
          }}
        >
          <Text style={styles.toolTitle}>{pat ? "Update" : "Set PAT"}</Text>
        </Pressable>
        {!!pat && (
          <Pressable
            style={[styles.toolButton, localStyles.smallBtn, localStyles.dangerBtn]}
            onPress={onClear}
          >
            <Text style={[styles.toolTitle, { color: colors.accent?.danger ?? "#FF6B6B" }]}>
              Clear
            </Text>
          </Pressable>
        )}
      </View>
    );
  }

  return (
    <View style={{ gap: 8 }}>
      <TextInput
        style={[
          styles.codeBlock,
          {
            color: colors.text.primary,
            borderColor: colors.accent.primary,
            borderWidth: 1,
            borderRadius: 8,
            padding: 10,
            fontSize: 13,
          },
        ]}
        value={draft}
        onChangeText={setDraft}
        placeholder="ghp_..."
        placeholderTextColor={colors.text.muted}
        autoCapitalize="none"
        autoCorrect={false}
        secureTextEntry
      />
      <View style={localStyles.patRow}>
        <Pressable
          style={[styles.toolButton, localStyles.smallBtn]}
          onPress={async () => {
            await onSave(draft.trim());
            setEditing(false);
          }}
        >
          <Text style={styles.toolTitle}>Save</Text>
        </Pressable>
        <Pressable
          style={[styles.toolButton, localStyles.smallBtn]}
          onPress={() => setEditing(false)}
        >
          <Text style={styles.toolTitle}>Cancel</Text>
        </Pressable>
      </View>
    </View>
  );
}

/* ─────────────────────────────────────────────
   Single workflow trigger row
───────────────────────────────────────────── */
function WorkflowRow({
  trigger,
  styles,
  colors,
  onPress,
}: {
  trigger: {
    id: string;
    label: string;
    status: "idle" | "loading" | "success" | "error";
    lastTriggeredTs?: number;
    error?: string;
  };
  styles: any;
  colors: any;
  onPress: () => void;
}) {
  const statusColor =
    trigger.status === "success"
      ? colors.accent?.success ?? "#4CAF50"
      : trigger.status === "error"
      ? colors.accent?.danger ?? "#FF6B6B"
      : trigger.status === "loading"
      ? colors.accent?.primary ?? "#4A9EFF"
      : colors.text?.muted ?? "#888";

  const statusLabel =
    trigger.status === "loading"
      ? "Triggering…"
      : trigger.status === "success"
      ? `Triggered${trigger.lastTriggeredTs ? " · " + new Date(trigger.lastTriggeredTs).toLocaleTimeString() : ""}`
      : trigger.status === "error"
      ? `Error`
      : "Not triggered";

  return (
    <View style={[styles.card, { marginBottom: 6 }]}>
      <View style={localStyles.triggerRow}>
        <View style={{ flex: 1 }}>
          <Text style={styles.cardTitle}>{trigger.label}</Text>
          <Text style={[styles.mutedText, { color: statusColor, marginTop: 2 }]}>
            {statusLabel}
          </Text>
          {trigger.status === "error" && trigger.error && (
            <Text style={[styles.dangerText, { marginTop: 2 }]} numberOfLines={2}>
              {trigger.error}
            </Text>
          )}
        </View>
        <Pressable
          style={[
            styles.toolButton,
            localStyles.triggerBtn,
            trigger.status === "loading" && { opacity: 0.5 },
          ]}
          disabled={trigger.status === "loading"}
          onPress={onPress}
        >
          <Text style={styles.toolTitle}>
            {trigger.status === "loading" ? "…" : "▶ Run"}
          </Text>
        </Pressable>
      </View>
      <Text style={[styles.mutedText, { fontSize: 10, marginTop: 4 }]}>
        {trigger.id}
      </Text>
    </View>
  );
}

/* ─────────────────────────────────────────────
   Collapsible section
───────────────────────────────────────────── */
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
      <Pressable style={styles.sectionHeader} onPress={() => setOpen((v) => !v)}>
        <Text style={styles.sectionTitle}>{title}</Text>
        <Text style={styles.sectionChevron}>{open ? "▾" : "▸"}</Text>
      </Pressable>
      {open && <View style={styles.sectionBody}>{children}</View>}
    </View>
  );
}

/* ─────────────────────────────────────────────
   Local layout styles
───────────────────────────────────────────── */
const localStyles = StyleSheet.create({
  headerRow: {
    flexDirection: "row",
    alignItems: "center",
    justifyContent: "space-between",
  },
  lockBtn: {
    paddingHorizontal: 12,
    paddingVertical: 6,
    minWidth: 0,
  },
  patRow: {
    flexDirection: "row",
    alignItems: "center",
    gap: 8,
  },
  smallBtn: {
    paddingHorizontal: 10,
    paddingVertical: 6,
    minWidth: 0,
  },
  dangerBtn: {
    borderColor: "transparent",
    backgroundColor: "transparent",
  },
  triggerRow: {
    flexDirection: "row",
    alignItems: "center",
    gap: 10,
  },
  triggerBtn: {
    paddingHorizontal: 12,
    paddingVertical: 8,
    minWidth: 60,
    alignItems: "center",
  },
});
