// /dev/dev-home.tsx  —  Dev Console (Workflow Triggers)
import React from "react";
import {
  ScrollView,
  View,
  Text,
  Pressable,
  TextInput,
  StyleSheet,
  Switch,
  AppState,
} from "react-native";
import { useRouter } from "expo-router";
import { useTheme } from "@/store/useTheme";
import { useDevStore } from "@/lib/dev/devStore";
import { createDevStyles } from "./devStyles";

/* ─────────────────────────────────────────────
   Input field definitions per backfill workflow
───────────────────────────────────────────── */
type FieldDef =
  | { key: string; label: string; type: "text"; placeholder: string; default: string }
  | { key: string; label: string; type: "toggle"; default: boolean };

const BACKFILL_CONFIGS: Record<string, { fields: FieldDef[] }> = {
  "epl_backfill.yml": {
    fields: [
      { key: "current_season", label: "Current Season (YYYY)", type: "text", placeholder: "e.g. 2025", default: "" },
      { key: "truncate_first", label: "Truncate tables first", type: "toggle", default: false },
    ],
  },
  "mls_backfill.yml": {
    fields: [
      { key: "start_season", label: "Start Season (YYYY)", type: "text", placeholder: "e.g. 2024", default: "2024" },
      { key: "end_season", label: "End Season (YYYY)", type: "text", placeholder: "e.g. 2025", default: "2025" },
      { key: "dry_run", label: "Dry run (no writes)", type: "toggle", default: false },
    ],
  },
  "atp_backfill.yml": {
    fields: [
      { key: "years", label: "Seasons to backfill", type: "text", placeholder: "e.g. 5", default: "5" },
      { key: "start_year", label: "Start Year (YYYY)", type: "text", placeholder: "optional", default: "" },
      { key: "end_year", label: "End Year (YYYY)", type: "text", placeholder: "optional", default: "" },
      { key: "truncate", label: "Truncate tables first", type: "toggle", default: true },
    ],
  },
  "pga_backfill.yml": {
    fields: [
      { key: "years", label: "Seasons to backfill", type: "text", placeholder: "e.g. 5", default: "5" },
      { key: "start_season", label: "Start Season (YYYY)", type: "text", placeholder: "optional", default: "" },
      { key: "end_season", label: "End Season (YYYY)", type: "text", placeholder: "optional", default: "" },
      { key: "truncate", label: "Truncate tables first", type: "toggle", default: true },
    ],
  },
};

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

function buildDefaultDraft(id: string): Record<string, string> {
  const config = BACKFILL_CONFIGS[id];
  if (!config) return {};
  const out: Record<string, string> = {};
  for (const f of config.fields) {
    out[f.key] = f.type === "toggle" ? (f.default ? "true" : "false") : f.default;
  }
  return out;
}

/* ─────────────────────────────────────────────
   Main screen
───────────────────────────────────────────── */
export default function DevHomeScreen() {
  const { colors } = useTheme();
  const styles = React.useMemo(() => createDevStyles(colors), [colors]);
  const router = useRouter();

  const { devUnlocked, githubPat, workflows, spTriggers, actions } = useDevStore();

  const [drafts, setDrafts] = React.useState<Record<string, Record<string, string>>>(() =>
    Object.fromEntries(BACKFILL_IDS.map((id) => [id, buildDefaultDraft(id)]))
  );

  const setField = (workflowId: string, key: string, value: string) => {
    setDrafts((prev) => ({
      ...prev,
      [workflowId]: { ...prev[workflowId], [key]: value },
    }));
  };

  React.useEffect(() => {
    actions.hydrateGithubPat();
  }, []);

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
        <Text style={styles.mutedText}>Tap the Pulse logo 4 times to unlock.</Text>
        <Pressable style={styles.toolButton} onPress={() => router.replace("/(tabs)/home")}>
          <Text style={styles.toolTitle}>⌂ Home</Text>
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
        <View style={localStyles.headerBtns}>
          <Pressable
            style={[styles.toolButton, localStyles.smallBtn]}
            onPress={() => router.replace("/(tabs)/home")}
          >
            <Text style={styles.toolTitle}>⌂ Home</Text>
          </Pressable>
          <Pressable
            style={[styles.toolButton, localStyles.smallBtn]}
            onPress={() => {
              actions.resetDevUnlock();
              router.replace("/(tabs)/home");
            }}
          >
            <Text style={styles.toolTitle}>🔒 Lock</Text>
          </Pressable>
        </View>
      </View>

      {/* ── GitHub Auth ── */}
      <Section title="GitHub Auth" styles={styles} defaultOpen>
        <PATInput
          pat={githubPat}
          colors={colors}
          styles={styles}
          onSave={async (val) => { await actions.setGithubPat(val); }}
          onClear={async () => { await actions.setGithubPat(""); }}
        />
        <Text style={[styles.mutedText, { marginTop: 6 }]}>
          Needs a GitHub PAT with{" "}
          <Text style={{ fontStyle: "italic" }}>workflow</Text> scope.
          Stored locally on this device only.
        </Text>
      </Section>

      {/* ── Daily Loaders ── */}
      <Section title="Daily Loaders" styles={styles} defaultOpen>
        {dailyTriggers.map((t) => (
          <SimpleWorkflowRow
            key={t.id}
            trigger={t}
            styles={styles}
            colors={colors}
            onPress={() => actions.triggerWorkflow(t.id)}
          />
        ))}
      </Section>

      {/* ── Stored Procedures ── */}
      <Section title="Stored Procedures" styles={styles} defaultOpen>
        {spTriggers.map((sp) => (
          <SpRow
            key={sp.id}
            sp={sp}
            styles={styles}
            colors={colors}
            onPress={() => actions.runSp(sp.id)}
          />
        ))}
      </Section>

      {/* ── Manual Backfills ── */}
      <Section title="Manual Backfills" styles={styles} defaultOpen={false}>
        <Text style={[styles.mutedText, { marginBottom: 8 }]}>
          Configure inputs then tap ▶ Run Backfill. Kicks off a GitHub Actions run immediately.
        </Text>
        {backfillTriggers.map((t) => (
          <BackfillRow
            key={t.id}
            trigger={t}
            draft={drafts[t.id] ?? {}}
            styles={styles}
            colors={colors}
            onFieldChange={(key, val) => setField(t.id, key, val)}
            onRun={() => {
              const inputs = Object.fromEntries(
                Object.entries(drafts[t.id] ?? {}).filter(([, v]) => v !== "")
              );
              actions.triggerWorkflow(t.id, inputs);
            }}
          />
        ))}
      </Section>
    </ScrollView>
  );
}

/* ─────────────────────────────────────────────
   Daily loader row — no inputs needed
───────────────────────────────────────────── */
function SimpleWorkflowRow({
  trigger, styles, colors, onPress,
}: {
  trigger: { id: string; label: string; status: string; lastTriggeredTs?: number; error?: string };
  styles: any; colors: any; onPress: () => void;
}) {
  const isLoading = trigger.status === "loading";
  const statusColor =
    trigger.status === "success" ? (colors.accent?.success ?? "#4CAF50") :
    trigger.status === "error"   ? (colors.accent?.danger  ?? "#FF6B6B") :
    trigger.status === "loading" ? (colors.accent?.primary ?? "#4A9EFF") :
                                   (colors.text?.muted     ?? "#888");
  const statusLabel =
    trigger.status === "loading" ? "Triggering…" :
    trigger.status === "success" ? `Triggered${trigger.lastTriggeredTs ? " · " + new Date(trigger.lastTriggeredTs).toLocaleTimeString() : ""}` :
    trigger.status === "error"   ? "Error" :
                                   "Not triggered";

  return (
    <View style={[styles.card, { marginBottom: 6 }]}>
      <View style={localStyles.triggerRow}>
        <View style={{ flex: 1 }}>
          <Text style={styles.cardTitle}>{trigger.label}</Text>
          <Text style={[styles.mutedText, { color: statusColor, marginTop: 2 }]}>{statusLabel}</Text>
          {trigger.status === "error" && trigger.error && (
            <Text style={[styles.dangerText, { marginTop: 2 }]} numberOfLines={2}>{trigger.error}</Text>
          )}
        </View>
        <Pressable
          style={[styles.toolButton, localStyles.runBtn, isLoading && { opacity: 0.5 }]}
          disabled={isLoading}
          onPress={onPress}
        >
          <Text style={styles.toolTitle}>{isLoading ? "…" : "▶ Run"}</Text>
        </Pressable>
      </View>
      <Text style={[styles.mutedText, { fontSize: 10, marginTop: 4 }]}>{trigger.id}</Text>
    </View>
  );
}

/* ─────────────────────────────────────────────
   Backfill row — input fields + run button
───────────────────────────────────────────── */
function BackfillRow({
  trigger, draft, styles, colors, onFieldChange, onRun,
}: {
  trigger: { id: string; label: string; status: string; lastTriggeredTs?: number; error?: string };
  draft: Record<string, string>;
  styles: any; colors: any;
  onFieldChange: (key: string, val: string) => void;
  onRun: () => void;
}) {
  const config = BACKFILL_CONFIGS[trigger.id];
  const isLoading = trigger.status === "loading";
  const statusColor =
    trigger.status === "success" ? (colors.accent?.success ?? "#4CAF50") :
    trigger.status === "error"   ? (colors.accent?.danger  ?? "#FF6B6B") :
    trigger.status === "loading" ? (colors.accent?.primary ?? "#4A9EFF") :
                                   (colors.text?.muted     ?? "#888");
  const statusLabel =
    trigger.status === "loading" ? "Triggering…" :
    trigger.status === "success" ? `Triggered${trigger.lastTriggeredTs ? " · " + new Date(trigger.lastTriggeredTs).toLocaleTimeString() : ""}` :
    trigger.status === "error"   ? "Error" :
                                   "Not triggered";

  return (
    <View style={[styles.card, { marginBottom: 10 }]}>
      <Text style={styles.cardTitle}>{trigger.label}</Text>
      <Text style={[styles.mutedText, { color: statusColor, marginTop: 2 }]}>{statusLabel}</Text>
      {trigger.status === "error" && trigger.error && (
        <Text style={[styles.dangerText, { marginTop: 2 }]} numberOfLines={3}>{trigger.error}</Text>
      )}

      {config?.fields.map((field) => (
        <View key={field.key} style={localStyles.fieldRow}>
          <Text style={[styles.keyText, { flex: 1 }]}>{field.label}</Text>
          {field.type === "toggle" ? (
            <Switch
              value={draft[field.key] === "true"}
              onValueChange={(v) => onFieldChange(field.key, v ? "true" : "false")}
              trackColor={{ true: colors.accent?.primary ?? "#4A9EFF" }}
            />
          ) : (
            <TextInput
              style={[
                localStyles.fieldInput,
                {
                  color: colors.text.primary,
                  borderColor: colors.border?.subtle ?? "#333",
                  backgroundColor: colors.surface?.screen ?? "#111",
                },
              ]}
              value={draft[field.key] ?? ""}
              onChangeText={(v) => onFieldChange(field.key, v)}
              placeholder={field.placeholder}
              placeholderTextColor={colors.text?.muted ?? "#888"}
              autoCapitalize="none"
              autoCorrect={false}
            />
          )}
        </View>
      ))}

      <Pressable
        style={[styles.toolButton, { marginTop: 10 }, isLoading && { opacity: 0.5 }]}
        disabled={isLoading}
        onPress={onRun}
      >
        <Text style={styles.toolTitle}>{isLoading ? "Triggering…" : "▶ Run Backfill"}</Text>
      </Pressable>

      <Text style={[styles.mutedText, { fontSize: 10, marginTop: 6 }]}>{trigger.id}</Text>
    </View>
  );
}

/* ─────────────────────────────────────────────
   Stored procedure trigger row
───────────────────────────────────────────── */
function SpRow({
  sp, styles, colors, onPress,
}: {
  sp: { id: string; label: string; call: string; status: string; lastTriggeredTs?: number; jobId?: string; error?: string };
  styles: any; colors: any; onPress: () => void;
}) {
  const isLoading = sp.status === "loading";
  const statusColor =
    sp.status === "success" ? (colors.accent?.success ?? "#4CAF50") :
    sp.status === "error"   ? (colors.accent?.danger  ?? "#FF6B6B") :
    sp.status === "loading" ? (colors.accent?.primary ?? "#4A9EFF") :
                               (colors.text?.muted    ?? "#888");
  const statusLabel =
    sp.status === "loading" ? "Starting…" :
    sp.status === "success" ? `Started${sp.lastTriggeredTs ? " · " + new Date(sp.lastTriggeredTs).toLocaleTimeString() : ""}` :
    sp.status === "error"   ? "Error" :
                               "Not run";

  return (
    <View style={[styles.card, { marginBottom: 6 }]}>
      <View style={localStyles.triggerRow}>
        <View style={{ flex: 1 }}>
          <Text style={styles.cardTitle}>{sp.label}</Text>
          <Text style={[styles.mutedText, { color: statusColor, marginTop: 2 }]}>{statusLabel}</Text>
          {sp.status === "success" && sp.jobId && (
            <Text style={[styles.mutedText, { fontSize: 10, marginTop: 2 }]} numberOfLines={1}>
              job: {sp.jobId}
            </Text>
          )}
          {sp.status === "error" && sp.error && (
            <Text style={[styles.dangerText, { marginTop: 2 }]} numberOfLines={2}>{sp.error}</Text>
          )}
        </View>
        <Pressable
          style={[styles.toolButton, localStyles.runBtn, isLoading && { opacity: 0.5 }]}
          disabled={isLoading}
          onPress={onPress}
        >
          <Text style={styles.toolTitle}>{isLoading ? "…" : "▶ Run"}</Text>
        </Pressable>
      </View>
      <Text style={[styles.mutedText, { fontSize: 10, marginTop: 4 }]}>{sp.call}</Text>
    </View>
  );
}

/* ─────────────────────────────────────────────
   PAT input with masked display
───────────────────────────────────────────── */
function PATInput({
  pat, colors, styles, onSave, onClear,
}: {
  pat: string; colors: any; styles: any;
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
          onPress={() => { setDraft(""); setEditing(true); }}
        >
          <Text style={styles.toolTitle}>{pat ? "Update" : "Set PAT"}</Text>
        </Pressable>
        {!!pat && (
          <Pressable
            style={[styles.toolButton, localStyles.smallBtn]}
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
          onPress={async () => { await onSave(draft.trim()); setEditing(false); }}
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
   Collapsible section
───────────────────────────────────────────── */
function Section({
  title, children, styles, defaultOpen = true,
}: {
  title: string; children: React.ReactNode; styles: any; defaultOpen?: boolean;
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
   Local styles
───────────────────────────────────────────── */
const localStyles = StyleSheet.create({
  headerRow: {
    flexDirection: "row",
    alignItems: "center",
    justifyContent: "space-between",
  },
  headerBtns: {
    flexDirection: "row",
    gap: 8,
  },
  smallBtn: {
    paddingHorizontal: 12,
    paddingVertical: 6,
    minWidth: 0,
  },
  patRow: {
    flexDirection: "row",
    alignItems: "center",
    gap: 8,
  },
  triggerRow: {
    flexDirection: "row",
    alignItems: "flex-start",
    gap: 10,
  },
  runBtn: {
    paddingHorizontal: 12,
    paddingVertical: 8,
    minWidth: 60,
    alignItems: "center",
  },
  fieldRow: {
    flexDirection: "row",
    alignItems: "center",
    marginTop: 10,
    gap: 10,
  },
  fieldInput: {
    flex: 1,
    borderWidth: 1,
    borderRadius: 6,
    paddingHorizontal: 8,
    paddingVertical: 6,
    fontSize: 13,
    maxWidth: 160,
  },
});
