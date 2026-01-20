// components/live/playerpropcard
import { View, Text, StyleSheet, Image } from "react-native";
import { useTheme } from "@/store/useTheme";
import { MarketRow } from "./MarketRow";
import { TEAM_LOGOS } from "@/constants/teamLogos";

export function PlayerPropCard({
  player,
  name,
  minutes,
  current,
}: any) {
  const { colors } = useTheme();

  return (
    <View style={styles.wrapper}>
      {/* LEFT ACCENT RAIL */}
      <View
        style={[
          styles.accent,
          { backgroundColor: colors.accent.primary },
        ]}
      />

      {/* CARD */}
      <View
        style={[
          styles.card,
          {
            backgroundColor: colors.surface.card,
            borderColor: colors.border.subtle,
          },
        ]}
      >
        <View style={styles.headerRow}>
          <Image
            source={{ uri: player.player_image_url }}
            style={styles.headshot}
          />

          <View style={styles.headerCenter}>
            <Text style={styles.name}>{name}</Text>
            <Text
              style={[
                styles.subline,
                { color: colors.text.muted },
              ]}
            >
              {minutes} min · PTS {current.pts} · REB {current.reb} · AST {current.ast}
            </Text>
          </View>

          <Image
            source={{ uri: TEAM_LOGOS[player.team_abbr] }}
            style={styles.teamLogo}
          />
        </View>

        {Object.entries(player.markets).map(
          ([market, marketData]: any) => {
            const currentByMarket: Record<string, number> = {
              pts: current.pts,
              ast: current.ast,
              reb: current.reb,
              "3pm": current.fg3m ?? 0,
            };

            const currentValue = currentByMarket[market];
            if (currentValue === undefined) return null;

            return (
              <MarketRow
                key={`${player.player_id}-${market}`}
                playerId={player.player_id}
                market={market}
                lines={marketData.lines}
                current={currentValue}
                playerName={name}
              />
            );
          }
        )}
      </View>
    </View>
  );
}

const styles = StyleSheet.create({
  wrapper: {
    flexDirection: "row",
    marginBottom: 14,
  },

  headerRow: {
    flexDirection: "row",
    alignItems: "center",
    justifyContent: "space-between",
  },

  headerCenter: {
    flex: 1,
    alignItems: "center",
  },

  headshot: {
    width: 36,
    height: 36,
    borderRadius: 18,
  },

  teamLogo: {
    width: 28,
    height: 28,
    resizeMode: "contain",
  },

  name: {
    fontSize: 16,
    fontWeight: "900",
    textAlign: "center",
  },

  subline: {
    fontSize: 12,
    fontWeight: "600",
    textAlign: "center",
  },

  accent: {
    width: 4,
    borderRadius: 4,
    marginRight: 10,
  },

  card: {
    flex: 1,
    borderWidth: 1,
    borderRadius: 16,
    padding: 14,
    gap: 10,

    shadowColor: "#000",
    shadowOpacity: 0.06,
    shadowRadius: 10,
    shadowOffset: { width: 0, height: 4 },

    elevation: 3,
  },
});