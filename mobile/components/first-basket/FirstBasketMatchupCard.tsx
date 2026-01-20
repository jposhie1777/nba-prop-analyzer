// mobile/components/first-basket/FirstBasketMatchupCard.tsx
import React from "react";
import { View, Text } from "react-native";
import { useTheme } from "@/store/useTheme";
import type {
  FirstBasketMatchup,
  FirstBasketSide,
} from "@/hooks/useFirstBasketMatchups";
import { FirstBasketHeader } from "./FirstBasketHeader";

/* ======================================================
   Player Row (stacked, mobile-first)
====================================================== */
function FirstBasketPlayerRow({
  side,
  highlight,
}: {
  side: FirstBasketSide;
  highlight?: boolean;
}) {
  const { colors } = useTheme();

  const fbPct = (side.firstBasketPct * 100).toFixed(1);
  const shotPct = (side.firstShotShare * 100).toFixed(0);

  const teamShare =
    side.playerTeamFirstBasketCount > 0
      ? side.playerFirstBasketCount / side.playerTeamFirstBasketCount
      : 0;

  return (
    <View
      style={{
        paddingVertical: 8,
        paddingHorizontal: 10,
        borderRadius: 12,
        backgroundColor: highlight
          ? colors.accent.soft
          : "transparent",
        marginBottom: 6,
      }}
    >
      {/* Player name */}
      <Text
        numberOfLines={1}
        style={{
          fontSize: 15,
          fontWeight: highlight ? "700" : "500",
          color: colors.text.primary,
        }}
      >
        {side.player}
      </Text>

      {/* Stats row */}
      <View
        style={{
          flexDirection: "row",
          gap: 14,
          marginTop: 2,
        }}
      >
        <Text style={{ fontSize: 12, color: colors.text.secondary }}>
          {fbPct}% FB
        </Text>

        <Text style={{ fontSize: 12, color: colors.text.secondary }}>
          {shotPct}% Shot
        </Text>
      </View>

      {/* Team share bar */}
      <View
        style={{
          height: 4,
          backgroundColor: colors.border.subtle,
          borderRadius: 2,
          marginTop: 6,
          overflow: "hidden",
        }}
      >
        <View
          style={{
            height: "100%",
            width: `${Math.min(teamShare * 100, 100)}%`,
            backgroundColor: colors.accent.primary,
            borderRadius: 2,
          }}
        />
      </View>
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
    <View style={{ marginTop: 12 }}>
      {/* Team header */}
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

      {players.map((p, i) => (
        <FirstBasketPlayerRow
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

  // Flatten + split rows into teams
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
      {/* Header */}
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

      {/* Teams stacked */}
      <TeamSection team={homeTeam} players={homePlayers} />
      <TeamSection team={awayTeam} players={awayPlayers} />
    </View>
  );
}