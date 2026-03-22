import { ActivityIndicator, Pressable, ScrollView, StyleSheet, Text, View } from "react-native";
import { useRouter } from "expo-router";

import {
  SoccerLeague,
  useSoccerUpcomingMatches,
} from "@/hooks/soccer/useSoccerMatchups";
import { useTheme } from "@/store/useTheme";
import { useSoccerLeagueBadges } from "@/hooks/soccer/useSoccerLeagueBadges";
import { resolveBadgeForTeam } from "@/utils/soccerDisplay";
import { MatchupSlugCard } from "@/components/soccer/MatchupSlugCard";
import { useSoccerBetslip } from "@/store/useSoccerBetslip";
import { useSoccerBetslipDrawer } from "@/store/useSoccerBetslipDrawer";

type Props = {
  league: SoccerLeague;
  title: string;
};

type OddsSide = "home" | "draw" | "away";

function decimalToAmerican(value?: number | null): number | null {
  if (value == null || Number.isNaN(value) || value <= 1) return null;
  if (value >= 2) return Math.round((value - 1) * 100);
  return Math.round(-100 / (value - 1));
}

function sideLabel(side: OddsSide): string {
  if (side === "home") return "Home";
  if (side === "draw") return "Draw";
  return "Away";
}

export function LeagueUpcomingMatchesScreen({ league, title }: Props) {
  const { colors } = useTheme();
  const router = useRouter();
  const betslipItems = useSoccerBetslip((s) => s.items);
  const addToBetslip = useSoccerBetslip((s) => s.add);
  const openDrawer = useSoccerBetslipDrawer((s) => s.open);
  const { data, loading, error, refetch } = useSoccerUpcomingMatches(league);
  const { data: badgeMap } = useSoccerLeagueBadges(league);

  return (
    <ScrollView style={styles.screen} contentContainerStyle={styles.content}>
      <View style={[styles.hero, { borderColor: colors.border.subtle }]}>
        <Text style={styles.eyebrow}>{title}</Text>
        <Text style={styles.h1}>Upcoming Matches</Text>
        <Text style={styles.sub}>Tap any matchup card for detailed breakdown.</Text>
      </View>

      {loading ? <ActivityIndicator color="#A78BFA" /> : null}

      {error ? (
        <Pressable onPress={refetch} style={[styles.errorBox, { borderColor: colors.border.subtle }]}>
          <Text style={styles.errorTitle}>Failed to load upcoming matches.</Text>
          <Text style={styles.errorText}>{error}</Text>
          <Text style={styles.errorRetry}>Tap to retry</Text>
        </Pressable>
      ) : null}

      {(data ?? []).map((match) => (
        (() => {
          const homeTeam = match.home_team ?? "Home";
          const awayTeam = match.away_team ?? "Away";
          const homeRecord = match.home_recent_form ?? "-";
          const awayRecord = match.away_recent_form ?? "-";
          const homeLogoUri = match.home_logo ?? resolveBadgeForTeam(league, homeTeam, badgeMap);
          const awayLogoUri = match.away_logo ?? resolveBadgeForTeam(league, awayTeam, badgeMap);
          const gameLabel = `${awayTeam} @ ${homeTeam}`;
          const getBetId = (side: OddsSide, price: number) =>
            `${league}-${match.match_id}-1x2-${side}-${price}-${match.start_time_utc ?? ""}`;
          const isSaved = (side: OddsSide, price: number) => betslipItems.some((item) => item.id === getBetId(side, price));
          const selectedOddsSide =
            (() => {
              const entries = [
                { side: "home" as const, pick: match.odds_summary?.home },
                { side: "draw" as const, pick: match.odds_summary?.draw },
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
              <MatchupSlugCard
                league={league}
                homeTeam={homeTeam}
                awayTeam={awayTeam}
                startTimeUtc={match.start_time_utc}
                homeRecord={homeRecord}
                awayRecord={awayRecord}
                homeLogoUri={homeLogoUri}
                awayLogoUri={awayLogoUri}
                oddsSummary={match.odds_summary}
                selectedOddsSide={selectedOddsSide}
                onCardPress={() =>
                  router.push({
                    pathname: `/(tabs)/${league}/match/[matchId]`,
                    params: {
                      matchId: String(match.match_id),
                      homeTeam,
                      awayTeam,
                      startTimeUtc: match.start_time_utc ?? "",
                      homeRecord: String(homeRecord),
                      awayRecord: String(awayRecord),
                      homeLogoUri: homeLogoUri ?? "",
                      awayLogoUri: awayLogoUri ?? "",
                    },
                  })
                }
                onOddsPress={(side) => {
                  const pick =
                    side === "home"
                      ? match.odds_summary?.home
                      : side === "draw"
                      ? match.odds_summary?.draw
                      : match.odds_summary?.away;
                  const price = pick?.odds_american ?? decimalToAmerican(pick?.odds_decimal);
                  if (price == null) return;
                  addToBetslip({
                    id: getBetId(side, price),
                    league: league.toUpperCase(),
                    game: gameLabel,
                    start_time_et: match.start_time_utc ?? undefined,
                    market: "1x2",
                    outcome: sideLabel(side),
                    line: null,
                    price,
                    bookmaker: pick?.bookie ?? "Best Book",
                  });
                  openDrawer();
                }}
              />
            </View>
          );
        })()
      ))}

      {!loading && !error && (data ?? []).length === 0 ? (
        <View style={[styles.emptyCard, { borderColor: colors.border.subtle }]}>
          <Text style={styles.emptyTitle}>No upcoming matches available.</Text>
          <Text style={styles.emptySub}>Once new match rows arrive, they will show here automatically.</Text>
        </View>
      ) : null}
    </ScrollView>
  );
}

const styles = StyleSheet.create({
  screen: { flex: 1, backgroundColor: "#050A18" },
  content: { padding: 16, gap: 10, paddingBottom: 40 },
  hero: { borderWidth: StyleSheet.hairlineWidth, borderRadius: 16, backgroundColor: "#071731", padding: 16, marginBottom: 8 },
  eyebrow: { color: "#90B3E9", fontSize: 11, fontWeight: "700" },
  h1: { color: "#E9F2FF", fontSize: 24, fontWeight: "800", marginTop: 8 },
  sub: { color: "#A7C0E8", marginTop: 6, lineHeight: 18, fontSize: 13 },
  card: { borderWidth: StyleSheet.hairlineWidth, borderRadius: 14, backgroundColor: "#0B1529", padding: 12 },
  errorBox: { borderWidth: StyleSheet.hairlineWidth, borderRadius: 12, backgroundColor: "#1F2937", padding: 12 },
  errorTitle: { color: "#FCA5A5", fontWeight: "700" },
  errorText: { color: "#FECACA", marginTop: 4, fontSize: 12 },
  errorRetry: { color: "#E5E7EB", marginTop: 8, fontSize: 12 },
  emptyCard: { borderWidth: StyleSheet.hairlineWidth, borderRadius: 12, backgroundColor: "#0B1529", padding: 14 },
  emptyTitle: { color: "#E5E7EB", fontWeight: "700" },
  emptySub: { color: "#A7C0E8", marginTop: 6, fontSize: 12 },
});
