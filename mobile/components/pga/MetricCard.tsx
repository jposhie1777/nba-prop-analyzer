import { View, Text, StyleSheet } from "react-native";
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
        },
      ]}
    >
      <Text style={[styles.title, { color: colors.text.primary }]}>
        {title}
      </Text>
      {subtitle ? (
        <Text style={[styles.subtitle, { color: colors.text.muted }]}>
          {subtitle}
        </Text>
      ) : null}
      <View style={styles.metrics}>
        {metrics.map((metric) => (
          <View key={metric.label} style={styles.metricRow}>
            <Text style={[styles.metricLabel, { color: colors.text.muted }]}>
              {metric.label}
            </Text>
            <Text style={[styles.metricValue, { color: colors.text.primary }]}>
              {metric.value ?? "—"}
            </Text>
          </View>
        ))}
      </View>
    </View>
  );
}

const styles = StyleSheet.create({
  card: {
    borderWidth: StyleSheet.hairlineWidth,
    borderRadius: 12,
    padding: 14,
    marginBottom: 12,
  },
  title: {
    fontSize: 15,
    fontWeight: "800",
  },
  subtitle: {
    marginTop: 4,
    fontSize: 12,
  },
  metrics: {
    marginTop: 10,
    gap: 6,
  },
  metricRow: {
    flexDirection: "row",
    justifyContent: "space-between",
  },
  metricLabel: {
    fontSize: 12,
  },
  metricValue: {
    fontSize: 12,
    fontWeight: "700",
  },
});
