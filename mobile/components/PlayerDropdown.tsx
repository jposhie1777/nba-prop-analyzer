// components/PlayerDropdown.tsx
import { View, Text, StyleSheet, TextInput, FlatList, Pressable } from "react-native";
import { useMemo, useState } from "react";
import { useTheme } from "@/store/useTheme";

type Props = {
  players: string[];
  value: string | null;
  onSelect: (player: string) => void;
};

export function PlayerDropdown({ players, value, onSelect }: Props) {
  const colors = useTheme((s) => s.colors);
  const styles = useMemo(() => makeStyles(colors), [colors]);

  const [open, setOpen] = useState(false);
  const [query, setQuery] = useState("");

  const filtered = useMemo(() => {
    if (!query) return players;
    return players.filter((p) =>
      p.toLowerCase().includes(query.toLowerCase())
    );
  }, [players, query]);

  return (
    <View>
      {/* SELECT */}
      <Pressable
        onPress={() => setOpen((v) => !v)}
        style={styles.select}
      >
        <Text style={styles.selectText}>
          {value ?? "Select Player"}
        </Text>
      </Pressable>

      {/* DROPDOWN */}
      {open && (
        <View style={styles.dropdown}>
          <TextInput
            placeholder="Search player…"
            placeholderTextColor={colors.text.muted}
            value={query}
            onChangeText={setQuery}
            style={styles.search}
          />

          <FlatList
            keyboardShouldPersistTaps="handled"
            data={filtered}
            keyExtractor={(item) => item}
            style={styles.list}
            renderItem={({ item }) => (
              <Pressable
                onPress={() => {
                  onSelect(item);
                  setOpen(false);
                  setQuery("");
                }}
                style={styles.option}
              >
                <Text style={styles.optionText}>{item}</Text>
              </Pressable>
            )}
          />
        </View>
      )}
    </View>
  );
}

const makeStyles = (colors: any) =>
  StyleSheet.create({
    select: {
      padding: 12,
      borderRadius: 12,
      backgroundColor: colors.surface.card,
      borderWidth: 1,
      borderColor: colors.border.subtle,
    },
    selectText: {
      fontWeight: "800",
      color: colors.text.primary,
    },
    dropdown: {
      marginTop: 6,
      borderRadius: 12,
      backgroundColor: colors.surface.card,
      borderWidth: 1,
      borderColor: colors.border.subtle,
      maxHeight: 280,
      overflow: "hidden",
    },
    search: {
      padding: 10,
      borderBottomWidth: 1,
      borderBottomColor: colors.border.subtle,
      color: colors.text.primary,
      fontWeight: "600",
    },
    list: {
      maxHeight: 220,
    },
    option: {
      padding: 12,
    },
    optionText: {
      fontWeight: "700",
      color: colors.text.primary,
    },
  });