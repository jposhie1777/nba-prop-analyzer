// components/live/LiveOdds
import { View, Text, StyleSheet } from "react-native";
import { useTheme } from "@/store/useTheme";
import { OddsValue, TeamSide } from "@/types/live";

type OddsBlock = {
  spread?: OddsValue[];
  total?: OddsValue[];
  moneyline?: OddsValue[];
};

type Props = {
  odds: OddsBlock;
  home: TeamSide;
  away: TeamSide;
};

export function LiveOdds({ odds, home, away }: Props) {
  const { colors } = useTheme();

  return (
    <View style={styles.wrap}>
      <BookGrid
        label="FanDuel"
        book="FD"
        odds={odds}
        home={home}
        away={away}
      />

      <BookGrid
        label="DraftKings"
        book="DK"
        odds={odds}
        home={home}
        away={away}
      />
    </View>
  );
}

/* ───────────────────────────── */

function BookGrid({
  label,
  book,
  odds,
  home,
  away,
}: {
  label: string;
  book: "FD" | "DK";
  odds: OddsBlock;
  home: TeamSide;
  away: TeamSide;
}) {
  const { colors } = useTheme();

  const spread = odds.spread?.find((o) => o.book === book);
  const total = odds.total?.find((o) => o.book === book);

  // moneyline assumed [home, away]
  const mlHome = odds.moneyline?.find((o) => o.book === book);
  const mlAway = odds.moneyline?.find(
    (o) => o.book === book && o !== mlHome
  );

  return (
    <View style={[styles.bookBox, { borderColor: colors.border.subtle }]}>
      <Text style={[styles.bookLabel, { color: colors.text.secondary }]}>
        {label}
      </Text>

      {/* Column headers */}
      <View style={styles.headerRow}>
        <Text style={styles.spacer} />
        <HeaderCell text="ML" />
        <HeaderCell text="SPREAD" />
        <HeaderCell text="TOTAL" />
      </View>

      {/* HOME ROW */}
      <OddsRow
        team={home}
        ml={mlHome}
        spread={spread}
        total={total}
      />

      {/* AWAY ROW */}
      <OddsRow
        team={away}
        ml={mlAway}
        spread={spread}
        total={total}
        invert
      />
    </View>
  );
}

/* ───────────────────────────── */

function OddsRow({
  team,
  ml,
  spread,
  total,
  invert,
}: {
  team: TeamSide;
  ml?: OddsValue;
  spread?: OddsValue;
  total?: OddsValue;
  invert?: boolean;
}) {
  const { colors } = useTheme();

  return (
    <View style={styles.row}>
      <Text style={[styles.team, { color: colors.text.primary }]}>
        {team.abbrev}
      </Text>

      <OddsBox value={fmtPrice(ml?.price)} />

      <OddsBox
        value={`${fmtLine(invert ? flip(spread?.line) : spread?.line)}`}
        sub={fmtPrice(spread?.price)}
      />

      <OddsBox
        value={`${invert ? "U" : "O"} ${total?.line ?? "—"}`}
        sub={fmtPrice(total?.price)}
      />
    </View>
  );
}

/* ───────────────────────────── */

function OddsBox({ value, sub }: { value: string; sub?: string }) {
  const { colors } = useTheme();

  return (
    <View
      style={[
        styles.box,
        {
          backgroundColor: colors.surface.card,
          borderColor: colors.border.subtle,
        },
      ]}
    >
      <Text style={[styles.boxTop, { color: colors.text.primary }]}>
        {value}
      </Text>
      {sub && (
        <Text style={[styles.boxBottom, { color: colors.text.secondary }]}>
          {sub}
        </Text>
      )}
    </View>
  );
}

/* ───────────────────────────── */

function HeaderCell({ text }: { text: string }) {
  const { colors } = useTheme();
  return (
    <Text style={[styles.header, { color: colors.text.muted }]}>
      {text}
    </Text>
  );
}

/* ───────────────────────────── */

function fmtPrice(v?: number) {
  if (v == null) return "—";
  return v > 0 ? `+${v}` : `${v}`;
}

function fmtLine(v?: number) {
  if (v == null) return "—";
  return v > 0 ? `+${v}` : `${v}`;
}

function flip(v?: number) {
  return v == null ? v : -v;
}

/* ───────────────────────────── */

const styles = StyleSheet.create({
  wrap: {
    flexDirection: "row",
    gap: 12,
    marginTop: 12,
  },

  bookBox: {
    flex: 1,
    borderWidth: 1,
    borderRadius: 14,
    padding: 10,
    gap: 8,
  },

  bookLabel: {
    fontSize: 13,
    fontWeight: "800",
    alignSelf: "center",
  },

  headerRow: {
    flexDirection: "row",
    alignItems: "center",
    paddingBottom: 6,
  },

  spacer: { width: 36 },

  header: {
    flex: 1,
    textAlign: "center",
    fontSize: 11,
    fontWeight: "800",
    letterSpacing: 1,
  },

  row: {
    flexDirection: "row",
    alignItems: "center",
    gap: 6,
  },

  team: {
    width: 36,
    fontSize: 12,
    fontWeight: "800",
    textAlign: "right",
  },

  box: {
    flex: 1,
    borderWidth: 1,
    borderRadius: 10,
    paddingVertical: 8,
    alignItems: "center",
  },

  boxTop: {
    fontSize: 14,
    fontWeight: "900",
  },

  boxBottom: {
    fontSize: 12,
    fontWeight: "700",
    marginTop: 2,
  },
});
