import { View, Text, TextInput, Pressable, StyleSheet, Platform } from "react-native";
import { Ionicons } from "@expo/vector-icons";
import { useTheme } from "@/store/useTheme";

export type SearchItem = {
  id: number;
  label: string;
  subLabel?: string;
};

type SearchPickerProps = {
  title: string;
  placeholder: string;
  query: string;
  onQueryChange: (value: string) => void;
  items: SearchItem[];
  selectedId?: number | null;
  onSelect: (item: SearchItem) => void;
  helperText?: string;
};

export function SearchPicker({
  title,
  placeholder,
  query,
  onQueryChange,
  items,
  selectedId,
  onSelect,
  helperText,
}: SearchPickerProps) {
  const { colors } = useTheme();

  return (
    <View style={styles.wrapper}>
      <Text style={[styles.title, { color: colors.text.primary }]}>{title}</Text>
      <View
        style={[
          styles.inputWrap,
          {
            backgroundColor: colors.surface.card,
            borderColor: colors.border.strong,
            ...Platform.select({
              ios: {
                shadowColor: "#000",
                shadowOffset: { width: 0, height: 1 },
                shadowOpacity: 0.04,
                shadowRadius: 4,
              },
              android: { elevation: 1 },
              default: {},
            }),
          },
        ]}
      >
        <Ionicons
          name="search"
          size={16}
          color={colors.text.muted}
          style={styles.searchIcon}
        />
        <TextInput
          value={query}
          onChangeText={onQueryChange}
          placeholder={placeholder}
          placeholderTextColor={colors.text.disabled ?? colors.text.muted}
          style={[styles.input, { color: colors.text.primary }]}
        />
      </View>
      {helperText ? (
        <Text style={[styles.helper, { color: colors.text.muted }]}>
          {helperText}
        </Text>
      ) : null}
      <View style={styles.list}>
        {items.slice(0, 8).map((item) => {
          const isSelected = item.id === selectedId;
          return (
            <Pressable
              key={item.id}
              onPress={() => onSelect(item)}
              style={({ pressed }) => [
                styles.item,
                {
                  backgroundColor: isSelected
                    ? colors.state.selected
                    : colors.surface.card,
                  borderColor: isSelected
                    ? colors.accent.primary
                    : colors.border.subtle,
                  opacity: pressed ? 0.85 : 1,
                },
              ]}
            >
              <View style={styles.itemContent}>
                <Text
                  style={[
                    styles.itemTitle,
                    { color: isSelected ? colors.accent.primary : colors.text.primary },
                  ]}
                >
                  {item.label}
                </Text>
                {item.subLabel ? (
                  <Text style={[styles.itemSub, { color: colors.text.muted }]}>
                    {item.subLabel}
                  </Text>
                ) : null}
              </View>
              {isSelected ? (
                <Ionicons
                  name="checkmark-circle"
                  size={20}
                  color={colors.accent.primary}
                />
              ) : null}
            </Pressable>
          );
        })}
      </View>
    </View>
  );
}

const styles = StyleSheet.create({
  wrapper: {
    marginBottom: 16,
  },
  title: {
    fontSize: 14,
    fontWeight: "700",
    marginBottom: 6,
  },
  inputWrap: {
    flexDirection: "row",
    alignItems: "center",
    borderWidth: 1,
    borderRadius: 12,
    paddingHorizontal: 12,
  },
  searchIcon: {
    marginRight: 8,
  },
  input: {
    flex: 1,
    paddingVertical: 11,
    fontSize: 14,
  },
  helper: {
    marginTop: 6,
    fontSize: 12,
    fontStyle: "italic",
  },
  list: {
    marginTop: 10,
    gap: 6,
  },
  item: {
    borderWidth: 1,
    borderRadius: 12,
    paddingVertical: 10,
    paddingHorizontal: 12,
    flexDirection: "row",
    alignItems: "center",
  },
  itemContent: {
    flex: 1,
  },
  itemTitle: {
    fontSize: 14,
    fontWeight: "600",
  },
  itemSub: {
    marginTop: 2,
    fontSize: 12,
  },
});
