// mobile/components/first-basket/FirstBasketMatchupCard.tsx
import React from "react";
import { View, Text } from "react-native";
import { useTheme } from "@/store/useTheme";
import type {
  FirstBasketMatchup,
  FirstBasketSide,
} from "@/hooks/useFirstBasketMatchups";
import { FirstBasketHeader } from "./FirstBasketHeader";
import { PlayerStatRow } from "./PlayerStatRow";

/* ======================================================
   Shared Stats Header (shown ONCE)
====================================================== */
function StatsHeader() {
  const { colors } = useTheme();

  return (
    <View
      style={{
        flexDirection: "row",
        justifyContent: "space-between",
        paddingHorizontal: 10,
        marginBottom: 4,
      }}
    >
      {["FB%", "Shot%", "FB", "Team FB"].map((label) => (
        <Text
          key={label}
          style={{
            fontSize: 11,
            color: colors.text.muted,
            minWidth: 52,
            textAlign: "center",
          }}
        >
          {label}
        </Text>
      ))}
    </View>
  );
}

/* ======================================================
   Team Section
====================================================== */
function TeamSection({
  team,
  players,
}: {
  team: string;
  players: FirstBasketSide[];
}) {
  const { colors } = useTheme();

  return (
    <View style={{ marginTop: 14 }}>
      {/* Team name */}
      <Text
        style={{
          fontSize: 13,
          fontWeight: "700",
          color: colors.text.primary,
          marginBottom: 6,
        }}
      >
        {team}
      </Text>

      {/* Shared header */}
      <StatsHeader />

      {/* Player rows */}
      {players.map((p, i) => (
        <PlayerStatRow
          key={p.player}
          side={p}
          highlight={i === 0}
        />
      ))}
    </View>
  );
}

/* ======================================================
   Matchup Card
====================================================== */
export function FirstBasketMatchupCard({
  matchup,
}: {
  matchup: FirstBasketMatchup;
}) {
  const { colors } = useTheme();

  const homeTeam = matchup.homeTeam;
  const awayTeam = matchup.awayTeam;

  const homeLogo = homeTeam
    ? `https://a.espncdn.com/i/teamlogos/nba/500/${homeTeam.toLowerCase()}.png`
    : undefined;

  const awayLogo = awayTeam
    ? `https://a.espncdn.com/i/teamlogos/nba/500/${awayTeam.toLowerCase()}.png`
    : undefined;

  // Split + sort players
  const homePlayers: FirstBasketSide[] = matchup.rows
    .map((r) => r.home)
    .filter(Boolean)
    .sort((a, b) => b.firstBasketPct - a.firstBasketPct)
    .slice(0, 5);

  const awayPlayers: FirstBasketSide[] = matchup.rows
    .map((r) => r.away)
    .filter(Boolean)
    .sort((a, b) => b.firstBasketPct - a.firstBasketPct)
    .slice(0, 5);

  return (
    <View
      style={{
        backgroundColor: colors.surface.card,
        borderRadius: 16,
        borderWidth: 1,
        borderColor: colors.border.subtle,
        padding: 12,
        marginBottom: 12,
      }}
    >
      {/* Matchup header */}
      <FirstBasketHeader
        homeTeam={homeTeam}
        awayTeam={awayTeam}
        homeLogo={homeLogo}
        awayLogo={awayLogo}
        homeWinPct={matchup.homeTipWinPct}
        awayWinPct={matchup.awayTipWinPct}
      />

      {/* Divider */}
      <View
        style={{
          height: 1,
          backgroundColor: colors.border.subtle,
          marginVertical: 8,
        }}
      />

      {/* Teams (stacked) */}
      <TeamSection team={homeTeam} players={homePlayers} />
      <TeamSection team={awayTeam} players={awayPlayers} />
    </View>
  );
}