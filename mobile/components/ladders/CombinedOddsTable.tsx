import { View, Text, StyleSheet } from "react-native";
import { useTheme } from "@/store/useTheme";
import { VendorBlock, Rung } from "@/hooks/useLadders";

type CombinedRow = {
  line: number;
  dk?: Rung;
  fd?: Rung;
  bestEdge: number;
};

function mergeVendorData(vendors: VendorBlock[]): CombinedRow[] {
  const rowMap = new Map<number, CombinedRow>();

  for (const vendor of vendors) {
    const vendorKey = vendor.vendor.toLowerCase();
    for (const rung of vendor.rungs) {
      const existing = rowMap.get(rung.line);
      if (existing) {
        if (vendorKey.includes("draftkings") || vendorKey === "dk") {
          existing.dk = rung;
        } else if (vendorKey.includes("fanduel") || vendorKey === "fd") {
          existing.fd = rung;
        }
        existing.bestEdge = Math.max(
          existing.bestEdge,
          rung.ladder_score
        );
      } else {
        const newRow: CombinedRow = {
          line: rung.line,
          bestEdge: rung.ladder_score,
        };
        if (vendorKey.includes("draftkings") || vendorKey === "dk") {
          newRow.dk = rung;
        } else if (vendorKey.includes("fanduel") || vendorKey === "fd") {
          newRow.fd = rung;
        }
        rowMap.set(rung.line, newRow);
      }
    }
  }

  return Array.from(rowMap.values()).sort((a, b) => a.line - b.line);
}

function formatOdds(odds: number | undefined): string {
  if (odds === undefined) return "-";
  return odds > 0 ? `+${odds}` : `${odds}`;
}

type Props = {
  vendors: VendorBlock[];
};

export function CombinedOddsTable({ vendors }: Props) {
  const { colors } = useTheme();
  const rows = mergeVendorData(vendors);

  if (rows.length === 0) return null;

  return (
    <View style={styles.container}>
      {/* Header */}
      <View style={[styles.headerRow, { borderBottomColor: colors.border.subtle }]}>
        <Text style={[styles.headerText, styles.lineCol, { color: colors.text.muted }]}>
          LINE
        </Text>
        <Text style={[styles.headerText, styles.oddsCol, { color: colors.text.muted }]}>
          DK
        </Text>
        <Text style={[styles.headerText, styles.oddsCol, { color: colors.text.muted }]}>
          FD
        </Text>
        <Text style={[styles.headerText, styles.edgeCol, { color: colors.text.muted }]}>
          EDGE
        </Text>
      </View>

      {/* Rows */}
      {rows.map((row) => {
        const dkOdds = row.dk?.odds;
        const fdOdds = row.fd?.odds;

        return (
          <View key={row.line} style={styles.dataRow}>
            <Text style={[styles.lineText, styles.lineCol, { color: colors.text.primary }]}>
              {row.line}
            </Text>
            <Text
              style={[
                styles.oddsText,
                styles.oddsCol,
                { color: dkOdds !== undefined && dkOdds < 0 ? "#22c55e" : colors.text.primary },
              ]}
            >
              {formatOdds(dkOdds)}
            </Text>
            <Text
              style={[
                styles.oddsText,
                styles.oddsCol,
                { color: fdOdds !== undefined && fdOdds < 0 ? "#22c55e" : colors.text.primary },
              ]}
            >
              {formatOdds(fdOdds)}
            </Text>
            <Text style={[styles.edgeText, styles.edgeCol, { color: "#22c55e" }]}>
              +{row.bestEdge.toFixed(1)}
            </Text>
          </View>
        );
      })}
    </View>
  );
}

const styles = StyleSheet.create({
  container: {
    marginTop: 4,
  },
  headerRow: {
    flexDirection: "row",
    paddingBottom: 6,
    marginBottom: 2,
    borderBottomWidth: 1,
  },
  headerText: {
    fontSize: 10,
    fontWeight: "600",
    letterSpacing: 0.3,
  },
  dataRow: {
    flexDirection: "row",
    paddingVertical: 5,
  },
  lineCol: {
    flex: 1,
  },
  oddsCol: {
    width: 55,
    textAlign: "right",
  },
  edgeCol: {
    width: 45,
    textAlign: "right",
  },
  lineText: {
    fontSize: 14,
    fontWeight: "600",
  },
  oddsText: {
    fontSize: 13,
    fontWeight: "500",
  },
  edgeText: {
    fontSize: 13,
    fontWeight: "600",
  },
});
