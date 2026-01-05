import { View, Text, StyleSheet, Image } from “react-native”;
import { useMemo } from “react”;
import { useTheme } from “@/store/useTheme”;

const TEAM_LOGOS: Record<string, string> = {
ATL: “https://a.espncdn.com/i/teamlogos/nba/500/atl.png”,
BOS: “https://a.espncdn.com/i/teamlogos/nba/500/bos.png”,
BKN: “https://a.espncdn.com/i/teamlogos/nba/500/bkn.png”,
CHA: “https://a.espncdn.com/i/teamlogos/nba/500/cha.png”,
CHI: “https://a.espncdn.com/i/teamlogos/nba/500/chi.png”,
CLE: “https://a.espncdn.com/i/teamlogos/nba/500/cle.png”,
DAL: “https://a.espncdn.com/i/teamlogos/nba/500/dal.png”,
DEN: “https://a.espncdn.com/i/teamlogos/nba/500/den.png”,
DET: “https://a.espncdn.com/i/teamlogos/nba/500/det.png”,
GSW: “https://a.espncdn.com/i/teamlogos/nba/500/gsw.png”,
HOU: “https://a.espncdn.com/i/teamlogos/nba/500/hou.png”,
IND: “https://a.espncdn.com/i/teamlogos/nba/500/ind.png”,
LAC: “https://a.espncdn.com/i/teamlogos/nba/500/lac.png”,
LAL: “https://a.espncdn.com/i/teamlogos/nba/500/lal.png”,
MEM: “https://a.espncdn.com/i/teamlogos/nba/500/mem.png”,
MIA: “https://a.espncdn.com/i/teamlogos/nba/500/mia.png”,
MIL: “https://a.espncdn.com/i/teamlogos/nba/500/mil.png”,
MIN: “https://a.espncdn.com/i/teamlogos/nba/500/min.png”,
NOP: “https://a.espncdn.com/i/teamlogos/nba/500/nop.png”,
NYK: “https://a.espncdn.com/i/teamlogos/nba/500/nyk.png”,
OKC: “https://a.espncdn.com/i/teamlogos/nba/500/okc.png”,
ORL: “https://a.espncdn.com/i/teamlogos/nba/500/orl.png”,
PHI: “https://a.espncdn.com/i/teamlogos/nba/500/phi.png”,
PHX: “https://a.espncdn.com/i/teamlogos/nba/500/phx.png”,
POR: “https://a.espncdn.com/i/teamlogos/nba/500/por.png”,
SAC: “https://a.espncdn.com/i/teamlogos/nba/500/sac.png”,
SAS: “https://a.espncdn.com/i/teamlogos/nba/500/sas.png”,
TOR: “https://a.espncdn.com/i/teamlogos/nba/500/tor.png”,
UTA: “https://a.espncdn.com/i/teamlogos/nba/500/uta.png”,
WAS: “https://a.espncdn.com/i/teamlogos/nba/500/was.png”,
};

type LiveGameCardProps = {
game: any;
};

export default function LiveGameCard({ game }: LiveGameCardProps) {
const colors = useTheme((s) => s.colors);
const styles = useMemo(() => makeStyles(colors), [colors]);

const homeTeam = game.competitions?.[0]?.competitors?.find(
(c: any) => c.homeAway === “home”
);
const awayTeam = game.competitions?.[0]?.competitors?.find(
(c: any) => c.homeAway === “away”
);

const isLive = game.status?.type?.state === “in”;
const isComplete = game.status?.type?.state === “post”;
const isPregame = game.status?.type?.state === “pre”;

const statusText = isLive
? game.status?.displayClock || “LIVE”
: isComplete
? “FINAL”
: new Date(game.date).toLocaleTimeString([], {
hour: “numeric”,
minute: “2-digit”,
});

const homeScore = homeTeam?.score || “0”;
const awayScore = awayTeam?.score || “0”;

const homeAbbrev = homeTeam?.team?.abbreviation || “???”;
const awayAbbrev = awayTeam?.team?.abbreviation || “???”;

return (
<View style={styles.card}>
{/* LIVE INDICATOR */}
{isLive && (
<View style={styles.liveStrip}>
<View style={styles.liveDot} />
<Text style={styles.liveText}>LIVE</Text>
</View>
)}

```
  {/* STATUS */}
  <View style={styles.statusRow}>
    <Text style={styles.statusText}>{statusText}</Text>
    {isLive && game.status?.period && (
      <Text style={styles.periodText}>
        Q{game.status.period}
      </Text>
    )}
  </View>

  {/* AWAY TEAM */}
  <View style={styles.teamRow}>
    <View style={styles.teamLeft}>
      {TEAM_LOGOS[awayAbbrev] ? (
        <Image
          source={{ uri: TEAM_LOGOS[awayAbbrev] }}
          style={styles.teamLogo}
        />
      ) : (
        <View style={styles.teamLogoPlaceholder} />
      )}
      <Text style={styles.teamName}>
        {awayTeam?.team?.displayName || awayAbbrev}
      </Text>
    </View>
    <Text
      style={[
        styles.score,
        !isPregame &&
          parseInt(awayScore) > parseInt(homeScore) &&
          styles.scoreWinning,
      ]}
    >
      {isPregame ? "" : awayScore}
    </Text>
  </View>

  {/* HOME TEAM */}
  <View style={styles.teamRow}>
    <View style={styles.teamLeft}>
      {TEAM_LOGOS[homeAbbrev] ? (
        <Image
          source={{ uri: TEAM_LOGOS[homeAbbrev] }}
          style={styles.teamLogo}
        />
      ) : (
        <View style={styles.teamLogoPlaceholder} />
      )}
      <Text style={styles.teamName}>
        {homeTeam?.team?.displayName || homeAbbrev}
      </Text>
    </View>
    <Text
      style={[
        styles.score,
        !isPregame &&
          parseInt(homeScore) > parseInt(awayScore) &&
          styles.scoreWinning,
      ]}
    >
      {isPregame ? "" : homeScore}
    </Text>
  </View>

  {/* BROADCAST INFO */}
  {game.competitions?.[0]?.broadcasts?.[0]?.names?.[0] && (
    <View style={styles.broadcastRow}>
      <Text style={styles.broadcastText}>
        📺 {game.competitions[0].broadcasts[0].names[0]}
      </Text>
    </View>
  )}
</View>
```

);
}

const makeStyles = (colors: any) =>
StyleSheet.create({
card: {
backgroundColor: colors.surface.card,
borderRadius: 16,
marginHorizontal: 14,
marginVertical: 8,
padding: 16,
borderWidth: 1,
borderColor: colors.border.subtle,
shadowColor: “#000”,
shadowOpacity: 0.15,
shadowRadius: 8,
shadowOffset: { width: 0, height: 4 },
elevation: 3,
overflow: “hidden”,
},

```
liveStrip: {
  position: "absolute",
  top: 0,
  left: 0,
  right: 0,
  height: 3,
  backgroundColor: colors.accent.danger,
  flexDirection: "row",
  alignItems: "center",
  paddingLeft: 8,
  gap: 6,
},

liveDot: {
  width: 6,
  height: 6,
  borderRadius: 3,
  backgroundColor: colors.accent.danger,
},

liveText: {
  fontSize: 10,
  fontWeight: "900",
  color: colors.accent.danger,
  letterSpacing: 0.5,
},

statusRow: {
  flexDirection: "row",
  justifyContent: "space-between",
  alignItems: "center",
  marginBottom: 12,
},

statusText: {
  fontSize: 13,
  fontWeight: "700",
  color: colors.text.muted,
  letterSpacing: 0.3,
},

periodText: {
  fontSize: 12,
  fontWeight: "800",
  color: colors.text.secondary,
  backgroundColor: colors.surface.cardSoft,
  paddingHorizontal: 8,
  paddingVertical: 3,
  borderRadius: 6,
},

teamRow: {
  flexDirection: "row",
  justifyContent: "space-between",
  alignItems: "center",
  paddingVertical: 8,
},

teamLeft: {
  flexDirection: "row",
  alignItems: "center",
  gap: 12,
  flex: 1,
},

teamLogo: {
  width: 32,
  height: 32,
  resizeMode: "contain",
},

teamLogoPlaceholder: {
  width: 32,
  height: 32,
  borderRadius: 8,
  backgroundColor: colors.surface.cardSoft,
},

teamName: {
  fontSize: 16,
  fontWeight: "700",
  color: colors.text.primary,
  flex: 1,
},

score: {
  fontSize: 24,
  fontWeight: "900",
  color: colors.text.secondary,
  minWidth: 40,
  textAlign: "right",
},

scoreWinning: {
  color: colors.text.primary,
},

broadcastRow: {
  marginTop: 8,
  paddingTop: 8,
  borderTopWidth: 1,
  borderTopColor: colors.border.subtle,
},

broadcastText: {
  fontSize: 12,
  color: colors.text.muted,
  fontWeight: "600",
},
```

});