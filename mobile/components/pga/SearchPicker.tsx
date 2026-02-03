import { View, Text, TextInput, Pressable, StyleSheet } from "react-native";
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
      <TextInput
        value={query}
        onChangeText={onQueryChange}
        placeholder={placeholder}
        placeholderTextColor={colors.text.muted}
        style={[
          styles.input,
          {
            backgroundColor: colors.surface.card,
            borderColor: colors.border.subtle,
            color: colors.text.primary,
          },
        ]}
      />
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
              style={[
                styles.item,
                {
                  backgroundColor: isSelected
                    ? colors.surface.cardSoft
                    : colors.surface.card,
                  borderColor: isSelected
                    ? colors.accent.primary
                    : colors.border.subtle,
                },
              ]}
            >
              <Text style={[styles.itemTitle, { color: colors.text.primary }]}>
                {item.label}
              </Text>
              {item.subLabel ? (
                <Text style={[styles.itemSub, { color: colors.text.muted }]}>
                  {item.subLabel}
                </Text>
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
  input: {
    borderWidth: 1,
    borderRadius: 10,
    paddingHorizontal: 12,
    paddingVertical: 10,
    fontSize: 14,
  },
  helper: {
    marginTop: 6,
    fontSize: 12,
  },
  list: {
    marginTop: 10,
    gap: 8,
  },
  item: {
    borderWidth: 1,
    borderRadius: 10,
    padding: 10,
  },
  itemTitle: {
    fontSize: 14,
    fontWeight: "700",
  },
  itemSub: {
    marginTop: 2,
    fontSize: 12,
  },
});
