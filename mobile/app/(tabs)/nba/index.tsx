// app/(tabs)/nba/index.tsx
import { ScrollView, Text, StyleSheet, Pressable } from "react-native";
import { useRouter } from "expo-router";
import { useTheme } from "@/store/useTheme";
import ThemeSelectorSection from "@/components/ThemeSelectorSection";

/* ======================================================
   Tile
====================================================== */
function Tile({ title, subtitle, onPress }: any) {
  const { colors } = useTheme();

  return (
    <Pressable
      onPress={onPress}
      style={[
        styles.tile,
        {
          backgroundColor: colors.surface.card,
          borderColor: colors.border.subtle,
        },
      ]}
    >
      <Text
        style={[
          styles.tileTitle,
          { color: colors.text.primary },
        ]}
      >
        {title}
      </Text>

      {!!subtitle && (
        <Text
          style={[
            styles.tileSub,
            { color: colors.text.muted },
          ]}
        >
          {subtitle}
        </Text>
      )}
    </Pressable>
  );
}

/* ======================================================
   Home
====================================================== */
export default function NbaHome() {
  const router = useRouter();
  const { colors } = useTheme();

  return (
    <ScrollView
      style={{
        flex: 1,
        backgroundColor: colors.surface.screen,
      }}
      contentContainerStyle={{
        padding: 16,
        paddingBottom: 40,
      }}
    >
      <ThemeSelectorSection title="Theme selector" />
      {/* ===========================
          Analytics
      ============================ */}
      <Text
        style={[
          styles.h1,
          {
            color: colors.text.primary,
            marginTop: 24,
          },
        ]}
      >
        Analytics
      </Text>

      <Tile
        title="Player Season Averages"
        subtitle="Points, rebounds, assists & more"
        onPress={() => router.push("/(tabs)/more/player-season-averages")}
      />

      <Tile
        title="Team Season Averages"
        subtitle="Team stats & standings"
        onPress={() => router.push("/(tabs)/more/team-season-averages")}
      />

      <Tile
        title="Opponent Position Defense"
        subtitle="Defense rankings by position"
        onPress={() => router.push("/(tabs)/more/opponent-position-defense")}
      />

      <Tile
        title="Lineups"
        subtitle="Projected & most common"
        onPress={() => router.push("/more/lineups")}
      />

      {/* ===========================
          More
      ============================ */}
      <Text
        style={[
          styles.h1,
          {
            color: colors.text.primary,
            marginTop: 24,
          },
        ]}
      >
        More
      </Text>

      <Tile
        title="Teams"
        subtitle="Team profiles & stats"
        onPress={() => router.push("/more/teams")}
      />

      <Tile
        title="Saved"
        subtitle="Saved bets & picks"
        onPress={() => router.push("/more/SavedScreen")}
      />

      <Tile
        title="Explore"
        subtitle="All features"
        onPress={() => router.push("/more/explore")}
      />
    </ScrollView>
  );
}

/* ======================================================
   Styles
====================================================== */
const styles = StyleSheet.create({
  h1: {
    fontSize: 18,
    fontWeight: "800",
    marginBottom: 10,
  },

  card: {
    borderWidth: StyleSheet.hairlineWidth,
    borderRadius: 12,
    padding: 14,
  },

  tile: {
    borderWidth: StyleSheet.hairlineWidth,
    borderRadius: 12,
    padding: 14,
    marginTop: 10,
  },

  tileTitle: {
    fontSize: 16,
    fontWeight: "800",
  },

  tileSub: {
    marginTop: 4,
    fontSize: 12,
  },
});
