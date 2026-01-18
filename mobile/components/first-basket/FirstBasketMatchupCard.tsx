// mobile/components/first-basket/FirstBasketMatchupCard.tsx
import React from "react";
import { View, Text } from "react-native";
import { useTheme } from "@/store/useTheme";
import type { FirstBasketMatchup, FirstBasketRow } from "@/hooks/useFirstBasketMatchups";
import { PlayerStatRow } from "./PlayerStatRow";
import { TextStyle } from "react-native";
import { FirstBasketHeader } from "./FirstBasketHeader";



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

  const title =
    matchup.homeTeam && matchup.awayTeam
      ? `${matchup.homeTeam} vs ${matchup.awayTeam}`
      : `Game ${matchup.gameId}`;

  // Hide rows where both sides are null
  const rows = matchup.rows.filter((r) => r.home || r.away);

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
      <FirstBasketHeader
        homeTeam={matchup.homeTeam}
        awayTeam={matchup.awayTeam}
        homeLogo={homeLogo}
        awayLogo={awayLogo}
        homeWinPct={matchup.homeTipWinPct}
        awayWinPct={matchup.awayTipWinPct}
      />


      {/* =======================
          HOME / AWAY LABELS
      ======================== */}
      <View
        style={{
          flexDirection: "row",
          marginTop: 6,
          marginBottom: 4,
        }}
      >
        <View style={{ flex: 1 }}>
          <Text
            style={{
              color: colors.text.muted,
              fontSize: 12,
              fontWeight: "700",
            }}
          >
            HOME
          </Text>
        </View>

        <View style={{ flex: 1, alignItems: "flex-end" }}>
          <Text
            style={{
              color: colors.text.muted,
              fontSize: 12,
              fontWeight: "700",
            }}
          >
            AWAY
          </Text>
        </View>
      </View>

      {/* =======================
          TABLE COLUMN HEADERS
      ======================== */}
      <View
        style={{
          flexDirection: "row",
          paddingBottom: 6,
          borderBottomWidth: 1,
          borderBottomColor: colors.border.subtle,
          marginBottom: 4,
        }}
      >
        {/* Home headers */}
        <View style={{ flex: 1, paddingRight: 6 }}>
          <View style={{ flexDirection: "row" }}>
            <Text style={colHeader(colors, 2)}>Player</Text>
            <Text style={colHeader(colors, 1, true)}>FB%</Text>
            <Text style={colHeader(colors, 1, true)}>Shot%</Text>
            <Text style={colHeader(colors, 1, true)}>FB</Text>
            <Text style={colHeader(colors, 1, true)}>Team</Text>
          </View>
        </View>

        {/* Away headers */}
        <View style={{ flex: 1, paddingLeft: 6 }}>
          <View style={{ flexDirection: "row" }}>
            <Text style={colHeader(colors, 2)}>Player</Text>
            <Text style={colHeader(colors, 1, true)}>FB%</Text>
            <Text style={colHeader(colors, 1, true)}>Shot%</Text>
            <Text style={colHeader(colors, 1, true)}>FB</Text>
            <Text style={colHeader(colors, 1, true)}>Team</Text>
          </View>
        </View>
      </View>

      {/* =======================
          DATA ROWS
      ======================== */}
      {rows.map((row: FirstBasketRow) => (
        <View
          key={row.rank}
          style={{
            flexDirection: "row",
            paddingVertical: 6,
            borderTopWidth: 1,
            borderTopColor: colors.border.subtle,
          }}
        >
          {/* HOME */}
          <View style={{ flex: 1, paddingRight: 6 }}>
            <PlayerStatRow side={row.home} />
          </View>

          {/* AWAY */}
          <View style={{ flex: 1, paddingLeft: 6 }}>
            <PlayerStatRow side={row.away} />
          </View>
        </View>
      ))}
    </View>
  );
}

/* =======================
   COLUMN HEADER STYLE
======================= */
function colHeader(
  colors: any,
  flex: number,
  rightAlign = false
): TextStyle {
  return {
    flex,
    fontSize: 11,
    color: colors.text.muted,
    textAlign: rightAlign ? "right" : "left",
  };
}
