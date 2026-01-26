// app/(tabs)/home.tsx
import { ScrollView, View, Text, StyleSheet, Pressable } from "react-native";
import { useRouter } from "expo-router";
import { useTheme } from "@/store/useTheme";
import { useParlayTracker } from "@/store/useParlayTracker";

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
export default function Home() {
  const router = useRouter();
  const { colors } = useTheme();
  const { tracked } = useParlayTracker();
  const trackedCount = Object.keys(tracked).length;

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
      {/* ===========================
          Today
      ============================ */}
      <Text
        style={[
          styles.h1,
          { color: colors.text.primary },
        ]}
      >
        Today
      </Text>

      <View
        style={[
          styles.card,
          {
            backgroundColor: colors.surface.card,
            borderColor: colors.border.subtle,
          },
        ]}
      >
        <Text style={{ color: colors.text.muted }}>
          (Coming next) Brief overview of today’s NBA games.
        </Text>
      </View>

      {/* ===========================
          Directory
      ============================ */}
      <Text
        style={[
          styles.h1,
          {
            color: colors.text.primary,
            marginTop: 18,
          },
        ]}
      >
        Directory
      </Text>

      <Tile
        title="Player Props"
        subtitle="Lines, odds, confidence"
        onPress={() => router.push("/(tabs)/props")}
      />

      <Tile
        title="Live"
        subtitle="Live scores, odds, box"
        onPress={() => router.push("/(tabs)/live")}
      />

      <Tile
        title="First Basket"
        subtitle="Matchups + projections"
        onPress={() => router.push("/(tabs)/first-basket")}
      />

      <Tile
        title="Trend Chart"
        subtitle="Player / market trends"
        onPress={() => router.push("/(tabs)/trends")}
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
        title={`Tracked Parlays${trackedCount > 0 ? ` (${trackedCount})` : ""}`}
        subtitle="Active bet tracking"
        onPress={() => router.push("/(tabs)/more/tracked-parlays")}
      />
  
      <Tile
        title="Lineups"
        subtitle="Projected & most common"
        onPress={() => router.push("/(more)/lineups")}
      />

      <Tile
        title="Teams"
        subtitle="Team profiles & stats"
        onPress={() => router.push("/(more)/teams")}
      />

      <Tile
        title="Saved"
        subtitle="Saved bets & picks"
        onPress={() => router.push("/(more)/SavedScreen")}
      />

      <Tile
        title="Explore"
        subtitle="All features"
        onPress={() => router.push("/(more)/explore")}
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