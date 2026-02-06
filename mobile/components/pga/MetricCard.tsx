import { View, Text, StyleSheet, Platform } from "react-native";
import { useTheme } from "@/store/useTheme";

type MetricItem = {
  label: string;
  value: string | number | null | undefined;
};

type MetricCardProps = {
  title: string;
  subtitle?: string;
  metrics: MetricItem[];
};

export function MetricCard({ title, subtitle, metrics }: MetricCardProps) {
  const { colors } = useTheme();

  return (
    <View
      style={[
        styles.card,
        {
          backgroundColor: colors.surface.card,
          borderColor: colors.border.subtle,
          ...Platform.select({
            ios: {
              shadowColor: "#000",
              shadowOffset: { width: 0, height: 1 },
              shadowOpacity: 0.05,
              shadowRadius: 6,
            },
            android: { elevation: 1 },
            default: {},
          }),
        },
      ]}
    >
      <View
        style={[styles.accentBar, { backgroundColor: colors.accent.primary }]}
      />
      <View style={styles.inner}>
        <Text style={[styles.title, { color: colors.text.primary }]}>
          {title}
        </Text>
        {subtitle ? (
          <Text style={[styles.subtitle, { color: colors.text.muted }]}>
            {subtitle}
          </Text>
        ) : null}
        <View
          style={[styles.divider, { backgroundColor: colors.border.subtle }]}
        />
        <View style={styles.metrics}>
          {metrics.map((metric) => (
            <View key={metric.label} style={styles.metricRow}>
              <Text
                style={[styles.metricLabel, { color: colors.text.muted }]}
              >
                {metric.label}
              </Text>
              <Text
                style={[styles.metricValue, { color: colors.text.primary }]}
              >
                {metric.value ?? "\u2014"}
              </Text>
            </View>
          ))}
        </View>
      </View>
    </View>
  );
}

const styles = StyleSheet.create({
  card: {
    borderWidth: StyleSheet.hairlineWidth,
    borderRadius: 14,
    marginBottom: 12,
    flexDirection: "row",
    overflow: "hidden",
  },
  accentBar: {
    width: 4,
  },
  inner: {
    flex: 1,
    padding: 14,
  },
  title: {
    fontSize: 15,
    fontWeight: "700",
    letterSpacing: -0.1,
  },
  subtitle: {
    marginTop: 3,
    fontSize: 12,
    lineHeight: 16,
  },
  divider: {
    height: StyleSheet.hairlineWidth,
    marginVertical: 10,
  },
  metrics: {
    gap: 7,
  },
  metricRow: {
    flexDirection: "row",
    justifyContent: "space-between",
    alignItems: "center",
  },
  metricLabel: {
    fontSize: 12,
    flex: 1,
  },
  metricValue: {
    fontSize: 13,
    fontWeight: "600",
    textAlign: "right",
    flexShrink: 0,
  },
});
