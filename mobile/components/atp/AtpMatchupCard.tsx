import { useMemo, useState } from "react";
import { Image, Pressable, StyleSheet, Text, View } from "react-native";

import type { AtpOddsSummary } from "@/hooks/atp/useAtpMatchups";

type OddsSide = "home" | "away";

type Props = {
  homePlayer: string;
  awayPlayer: string;
  startTimeUtc?: string | null;
  tournamentName?: string | null;
  roundName?: string | null;
  homeRank?: string | null;
  awayRank?: string | null;
  homeHeadshotUrl?: string | null;
  awayHeadshotUrl?: string | null;
  oddsSummary?: AtpOddsSummary;
  onCardPress?: () => void;
  onOddsPress?: (side: OddsSide) => void;
  selectedOddsSide?: OddsSide | null;
};

function formatDay(value?: string | null) {
  if (!value) return "TBD";
  const date = new Date(value);
  if (Number.isNaN(date.getTime())) return "TBD";
  return date.toLocaleDateString(undefined, {
    weekday: "short",
    month: "short",
    day: "numeric",
  });
}

function formatTime(value?: string | null) {
  if (!value) return "TBD";
  const date = new Date(value);
  if (Number.isNaN(date.getTime())) return "TBD";
  return date.toLocaleTimeString(undefined, {
    hour: "numeric",
    minute: "2-digit",
  });
}

function initials(value: string): string {
  const parts = value.split(" ").filter(Boolean);
  if (!parts.length) return "?";
  if (parts.length === 1) return parts[0].slice(0, 2).toUpperCase();
  return `${parts[0][0] ?? ""}${parts[parts.length - 1][0] ?? ""}`.toUpperCase();
}

function decimalToAmerican(value?: number | null): number | null {
  if (value == null || Number.isNaN(value) || value <= 1) return null;
  if (value >= 2) return Math.round((value - 1) * 100);
  return Math.round(-100 / (value - 1));
}

function formatAmericanOdds(value?: number | null): string {
  if (value == null) return "—";
  if (value > 0) return `+${value}`;
  return String(value);
}

function formatOddsCell(value?: { odds_decimal?: number | null; odds_american?: number | null } | null): string {
  if (!value) return "—";
  const american = value.odds_american ?? decimalToAmerican(value.odds_decimal);
  if (american == null) return "—";
  return formatAmericanOdds(american);
}

function oddsLabel(playerName: string): string {
  const parts = playerName.trim().split(/\s+/);
  const last = parts[parts.length - 1] || playerName;
  return last.toUpperCase();
}

function PlayerBadge({
  playerName,
  rank,
  align,
  headshotUrl,
}: {
  playerName: string;
  rank?: string | null;
  align: "left" | "right";
  headshotUrl?: string | null;
}) {
  const [loadFailed, setLoadFailed] = useState(false);
  const uri = useMemo(() => headshotUrl ?? null, [headshotUrl]);

  return (
    <View style={[styles.playerBadge, align === "right" ? { alignItems: "flex-end" } : null]}>
      {uri && !loadFailed ? (
        <Image
          source={{ uri }}
          style={styles.headshot}
          onError={() => setLoadFailed(true)}
        />
      ) : (
        <View style={styles.fallbackHeadshot}>
          <Text style={styles.fallbackText}>{initials(playerName)}</Text>
        </View>
      )}
      <Text
        style={[styles.playerName, align === "right" ? styles.playerNameRight : null]}
        numberOfLines={1}
      >
        {playerName}
      </Text>
      {rank ? (
        <Text style={[styles.rankText, align === "right" ? styles.playerNameRight : null]} numberOfLines={1}>
          {rank}
        </Text>
      ) : null}
    </View>
  );
}

export function AtpMatchupCard({
  homePlayer,
  awayPlayer,
  startTimeUtc,
  tournamentName,
  roundName,
  homeRank,
  awayRank,
  homeHeadshotUrl,
  awayHeadshotUrl,
  oddsSummary,
  onCardPress,
  onOddsPress,
  selectedOddsSide,
}: Props) {
  const hasOdds = Boolean(oddsSummary?.home || oddsSummary?.away);
  const interactiveOdds = typeof onOddsPress === "function";

  const body = (
    <View style={styles.root}>
      <View style={styles.topRow}>
        <PlayerBadge
          playerName={homePlayer}
          rank={homeRank}
          align="left"
          headshotUrl={homeHeadshotUrl}
        />
        <View style={styles.center}>
          <Text style={styles.dayText}>{formatDay(startTimeUtc)}</Text>
          <Text style={styles.timeText}>{formatTime(startTimeUtc)}</Text>
          {tournamentName ? <Text style={styles.metaText}>{tournamentName}</Text> : null}
          {roundName ? <Text style={styles.metaText}>{roundName}</Text> : null}
        </View>
        <PlayerBadge
          playerName={awayPlayer}
          rank={awayRank}
          align="right"
          headshotUrl={awayHeadshotUrl}
        />
      </View>

      {hasOdds ? (
        <View style={styles.oddsWrap}>
          <View style={styles.oddsHeaderRow}>
            <Text style={styles.oddsHeader}>{oddsLabel(homePlayer)}</Text>
            <Text style={styles.oddsHeader}>{oddsLabel(awayPlayer)}</Text>
          </View>
          {interactiveOdds ? (
            <View style={styles.oddsValueRow}>
              {(
                [
                  { side: "home", value: oddsSummary?.home },
                  { side: "away", value: oddsSummary?.away },
                ] as const
              ).map(({ side, value }) => {
                const oddsText = formatOddsCell(value);
                const disabled = oddsText === "—";
                return (
                  <Pressable
                    key={side}
                    disabled={disabled}
                    onPress={() => onOddsPress?.(side)}
                    style={[
                      styles.oddsButton,
                      selectedOddsSide === side ? styles.oddsButtonSelected : null,
                      disabled ? styles.oddsButtonDisabled : null,
                    ]}
                  >
                    <Text
                      style={[
                        styles.oddsButtonText,
                        selectedOddsSide === side ? styles.oddsButtonTextSelected : null,
                      ]}
                    >
                      {oddsText}
                    </Text>
                  </Pressable>
                );
              })}
            </View>
          ) : (
            <View style={styles.oddsValueRow}>
              <Text style={styles.oddsValue}>{formatOddsCell(oddsSummary?.home)}</Text>
              <Text style={styles.oddsValue}>{formatOddsCell(oddsSummary?.away)}</Text>
            </View>
          )}
        </View>
      ) : null}
    </View>
  );

  if (onCardPress) {
    return <Pressable onPress={onCardPress}>{body}</Pressable>;
  }
  return body;
}

const styles = StyleSheet.create({
  root: {
    width: "100%",
    gap: 10,
  },
  topRow: {
    flexDirection: "row",
    alignItems: "center",
    justifyContent: "space-between",
    gap: 8,
  },
  playerBadge: {
    flex: 1.7,
    gap: 4,
  },
  headshot: {
    width: 30,
    height: 30,
    borderRadius: 15,
    backgroundColor: "#111827",
  },
  fallbackHeadshot: {
    width: 30,
    height: 30,
    borderRadius: 15,
    backgroundColor: "#1F2937",
    alignItems: "center",
    justifyContent: "center",
  },
  fallbackText: {
    color: "#E5E7EB",
    fontSize: 10,
    fontWeight: "800",
  },
  playerName: {
    color: "#E5E7EB",
    fontSize: 13,
    fontWeight: "700",
  },
  playerNameRight: {
    textAlign: "right",
  },
  rankText: {
    color: "#94A3B8",
    fontSize: 11,
    fontWeight: "600",
  },
  center: {
    flex: 1.6,
    alignItems: "center",
    gap: 2,
  },
  dayText: {
    color: "#B7C5DD",
    fontSize: 12,
    fontWeight: "600",
  },
  timeText: {
    color: "#F8FAFC",
    fontSize: 18,
    fontWeight: "800",
  },
  metaText: {
    color: "#93C5FD",
    fontSize: 10,
    fontWeight: "600",
    textAlign: "center",
  },
  oddsWrap: {
    borderTopWidth: StyleSheet.hairlineWidth,
    borderTopColor: "#1E293B",
    paddingTop: 8,
    gap: 4,
  },
  oddsHeaderRow: {
    flexDirection: "row",
    justifyContent: "space-between",
    gap: 8,
  },
  oddsHeader: {
    flex: 1,
    textAlign: "center",
    color: "#94A3B8",
    fontSize: 10,
    fontWeight: "700",
    textTransform: "uppercase",
    letterSpacing: 0.2,
  },
  oddsValueRow: {
    flexDirection: "row",
    justifyContent: "space-between",
    gap: 8,
  },
  oddsValue: {
    flex: 1,
    textAlign: "center",
    color: "#E2E8F0",
    fontSize: 13,
    fontWeight: "700",
  },
  oddsButton: {
    flex: 1,
    borderWidth: StyleSheet.hairlineWidth,
    borderColor: "#334155",
    borderRadius: 8,
    backgroundColor: "#0F172A",
    paddingVertical: 6,
    alignItems: "center",
    justifyContent: "center",
  },
  oddsButtonSelected: {
    borderColor: "#3B82F6",
    backgroundColor: "rgba(59,130,246,0.2)",
  },
  oddsButtonDisabled: {
    opacity: 0.5,
  },
  oddsButtonText: {
    color: "#BFDBFE",
    fontSize: 13,
    fontWeight: "800",
  },
  oddsButtonTextSelected: {
    color: "#DBEAFE",
  },
});
