import {
View,
Text,
StyleSheet,
FlatList,
ActivityIndicator,
} from “react-native”;
import { useMemo } from “react”;
import { GestureHandlerRootView } from “react-native-gesture-handler”;

import { useTheme } from “@/store/useTheme”;
import { useLiveScores } from “@/lib/useLiveScores”;
import LiveGameCard from “@/components/LiveGameCard”;

export default function LiveGamesScreen() {
const colors = useTheme((s) => s.colors);
const styles = useMemo(() => makeStyles(colors), [colors]);

const { snapshot, connected, isStale } = useLiveScores();

// Sort games: live games first, then by start time
const sortedGames = useMemo(() => {
if (!snapshot?.games) return [];

```
return [...snapshot.games].sort((a, b) => {
  // Live games first
  const aLive = a.status?.type?.state === "in";
  const bLive = b.status?.type?.state === "in";
  
  if (aLive && !bLive) return -1;
  if (!aLive && bLive) return 1;

  // Then by start time
  return new Date(a.date).getTime() - new Date(b.date).getTime();
});
```

}, [snapshot?.games]);

const renderItem = ({ item }: { item: any }) => (
<LiveGameCard game={item} />
);

if (!snapshot) {
return (
<GestureHandlerRootView style={styles.root}>
<View style={styles.center}>
<ActivityIndicator size="large" color={colors.accent.primary} />
<Text style={styles.loadingText}>Connecting to live scores…</Text>
</View>
</GestureHandlerRootView>
);
}

return (
<GestureHandlerRootView style={styles.root}>
<View style={styles.screen}>
{/* CONNECTION STATUS */}
<View style={styles.statusBar}>
<View style={styles.statusRow}>
<View
style={[
styles.statusDot,
{
backgroundColor: connected && !isStale
? colors.accent.success
: colors.accent.danger,
},
]}
/>
<Text style={styles.statusText}>
{connected && !isStale
? “Live”
: isStale
? “Reconnecting…”
: “Disconnected”}
</Text>
</View>

```
      {snapshot.meta.source_updated_at && (
        <Text style={styles.updateTime}>
          Updated: {new Date(snapshot.meta.source_updated_at).toLocaleTimeString()}
        </Text>
      )}
    </View>

    {/* GAMES LIST */}
    {sortedGames.length === 0 ? (
      <View style={styles.center}>
        <Text style={styles.emptyText}>No games today</Text>
      </View>
    ) : (
      <FlatList
        data={sortedGames}
        keyExtractor={(item) => item.id}
        renderItem={renderItem}
        showsVerticalScrollIndicator={false}
        contentContainerStyle={styles.list}
        ListFooterComponent={<View style={{ height: 40 }} />}
      />
    )}
  </View>
</GestureHandlerRootView>
```

);
}

const makeStyles = (colors: any) =>
StyleSheet.create({
root: { flex: 1 },

```
screen: {
  flex: 1,
  backgroundColor: colors.surface.screen,
},

center: {
  flex: 1,
  justifyContent: "center",
  alignItems: "center",
  gap: 12,
},

loadingText: {
  color: colors.text.muted,
  fontSize: 14,
  fontWeight: "600",
},

statusBar: {
  flexDirection: "row",
  justifyContent: "space-between",
  alignItems: "center",
  paddingHorizontal: 16,
  paddingVertical: 12,
  backgroundColor: colors.surface.card,
  borderBottomWidth: 1,
  borderBottomColor: colors.border.subtle,
},

statusRow: {
  flexDirection: "row",
  alignItems: "center",
  gap: 8,
},

statusDot: {
  width: 8,
  height: 8,
  borderRadius: 4,
},

statusText: {
  fontSize: 14,
  fontWeight: "700",
  color: colors.text.primary,
},

updateTime: {
  fontSize: 12,
  color: colors.text.muted,
},

emptyText: {
  fontSize: 16,
  color: colors.text.muted,
  fontWeight: "600",
},

list: {
  paddingTop: 8,
},
```

});