import { ActivityIndicator, Pressable, ScrollView, StyleSheet, Text, View } from "react-native";
import { useRouter } from "expo-router";

import { AtpMatchupCard } from "@/components/atp/AtpMatchupCard";
import { useAtpUpcomingMatches } from "@/hooks/atp/useAtpMatchups";
import { useAtpBetslip } from "@/store/useAtpBetslip";
import { useAtpBetslipDrawer } from "@/store/useAtpBetslipDrawer";
import { useTheme } from "@/store/useTheme";

type OddsSide = "home" | "away";

function decimalToAmerican(value?: number | null): number | null {
  if (value == null || Number.isNaN(value) || value <= 1) return null;
  if (value >= 2) return Math.round((value - 1) * 100);
  return Math.round(-100 / (value - 1));
}

function marketOutcomeLabel(side: OddsSide, homePlayer: string, awayPlayer: string): string {
  return side === "home" ? homePlayer : awayPlayer;
}

function formatMatchTime(value?: string | null): string | undefined {
  if (!value) return undefined;
  const date = new Date(value);
  if (Number.isNaN(date.getTime())) return undefined;
  return date.toLocaleString();
}

export function AtpUpcomingMatchesScreen() {
  const { colors } = useTheme();
  const router = useRouter();
  const betslipItems = useAtpBetslip((s) => s.items);
  const addToBetslip = useAtpBetslip((s) => s.add);
  const openDrawer = useAtpBetslipDrawer((s) => s.open);
  const { data, loading, error, refetch } = useAtpUpcomingMatches(100, 14);

  return (
    <ScrollView style={styles.screen} contentContainerStyle={styles.content}>
      <View style={[styles.hero, { borderColor: colors.border.subtle }]}>
        <Text style={styles.eyebrow}>ATP</Text>
        <Text style={styles.h1}>Upcoming Matches</Text>
        <Text style={styles.sub}>
          Select odds to save picks to your ATP betslip, or open any matchup for full details.
        </Text>
      </View>

      {loading ? <ActivityIndicator color="#A78BFA" /> : null}

      {error ? (
        <Pressable onPress={refetch} style={[styles.errorBox, { borderColor: colors.border.subtle }]}>
          <Text style={styles.errorTitle}>Failed to load ATP matches.</Text>
          <Text style={styles.errorText}>{error}</Text>
          <Text style={styles.errorRetry}>Tap to retry</Text>
        </Pressable>
      ) : null}

      {(data ?? []).map((match) => {
        const homePlayer = match.home_team ?? "Player 1";
        const awayPlayer = match.away_team ?? "Player 2";
        const gameLabel = `${awayPlayer} vs ${homePlayer}`;
        const getBetId = (side: OddsSide, price: number) =>
          `atp-${match.match_id}-moneyline-${side}-${price}-${match.start_time_utc ?? ""}`;
        const isSaved = (side: OddsSide, price: number) =>
          betslipItems.some((item) => item.id === getBetId(side, price));
        const selectedOddsSide =
          (() => {
            const entries = [
              { side: "home" as const, pick: match.odds_summary?.home },
              { side: "away" as const, pick: match.odds_summary?.away },
            ];
            for (const entry of entries) {
              const price = entry.pick?.odds_american ?? decimalToAmerican(entry.pick?.odds_decimal);
              if (price == null) continue;
              if (isSaved(entry.side, price)) return entry.side;
            }
            return null;
          })();

        return (
          <View key={match.match_id} style={[styles.card, { borderColor: colors.border.subtle }]}>
            <AtpMatchupCard
              homePlayer={homePlayer}
              awayPlayer={awayPlayer}
              startTimeUtc={match.start_time_utc}
              tournamentName={match.tournament_name}
              roundName={match.round_name}
              homeRank={match.home_rank}
              awayRank={match.away_rank}
              homeHeadshotUrl={match.home_headshot_url}
              awayHeadshotUrl={match.away_headshot_url}
              oddsSummary={match.odds_summary}
              selectedOddsSide={selectedOddsSide}
              onCardPress={() =>
                router.push({
                  pathname: "/(tabs)/atp/match/[matchId]",
                  params: {
                    matchId: String(match.match_id),
                    homePlayer,
                    awayPlayer,
                    startTimeUtc: match.start_time_utc ?? "",
                    tournamentName: match.tournament_name ?? "",
                    roundName: match.round_name ?? "",
                    homeRank: match.home_rank ?? "",
                    awayRank: match.away_rank ?? "",
                    homeHeadshotUrl: match.home_headshot_url ?? "",
                    awayHeadshotUrl: match.away_headshot_url ?? "",
                  },
                })
              }
              onOddsPress={(side) => {
                const pick = side === "home" ? match.odds_summary?.home : match.odds_summary?.away;
                const price = pick?.odds_american ?? decimalToAmerican(pick?.odds_decimal);
                if (price == null) return;
                addToBetslip({
                  id: getBetId(side, price),
                  player: marketOutcomeLabel(side, homePlayer, awayPlayer),
                  playerId: null,
                  opponent: side === "home" ? awayPlayer : homePlayer,
                  tournamentName: match.tournament_name ?? undefined,
                  round: match.round_name ?? undefined,
                  matchTime: formatMatchTime(match.start_time_utc),
                  createdAt: new Date().toISOString(),
                  market: "Moneyline",
                  outcome: marketOutcomeLabel(side, homePlayer, awayPlayer),
                  line: null,
                  price,
                  bookmaker: pick?.bookie ?? "Best Book",
                  matchId: match.match_id,
                  game: gameLabel,
                });
                openDrawer();
              }}
            />
          </View>
        );
      })}

      {!loading && !error && (data ?? []).length === 0 ? (
        <View style={[styles.emptyCard, { borderColor: colors.border.subtle }]}>
          <Text style={styles.emptyTitle}>No upcoming ATP matches available.</Text>
          <Text style={styles.emptySub}>Once fresh match rows arrive in BigQuery, they will show here.</Text>
        </View>
      ) : null}
    </ScrollView>
  );
}

const styles = StyleSheet.create({
  screen: { flex: 1, backgroundColor: "#050A18" },
  content: { padding: 16, gap: 10, paddingBottom: 40 },
  hero: {
    borderWidth: StyleSheet.hairlineWidth,
    borderRadius: 16,
    backgroundColor: "#071731",
    padding: 16,
    marginBottom: 8,
  },
  eyebrow: { color: "#90B3E9", fontSize: 11, fontWeight: "700" },
  h1: { color: "#E9F2FF", fontSize: 24, fontWeight: "800", marginTop: 8 },
  sub: { color: "#A7C0E8", marginTop: 6, lineHeight: 18, fontSize: 13 },
  card: {
    borderWidth: StyleSheet.hairlineWidth,
    borderRadius: 14,
    backgroundColor: "#0B1529",
    padding: 12,
  },
  errorBox: {
    borderWidth: StyleSheet.hairlineWidth,
    borderRadius: 12,
    backgroundColor: "#1F2937",
    padding: 12,
  },
  errorTitle: { color: "#FCA5A5", fontWeight: "700" },
  errorText: { color: "#FECACA", marginTop: 4, fontSize: 12 },
  errorRetry: { color: "#E5E7EB", marginTop: 8, fontSize: 12 },
  emptyCard: {
    borderWidth: StyleSheet.hairlineWidth,
    borderRadius: 12,
    backgroundColor: "#0B1529",
    padding: 14,
  },
  emptyTitle: { color: "#E5E7EB", fontWeight: "700" },
  emptySub: { color: "#A7C0E8", marginTop: 6, fontSize: 12 },
});
