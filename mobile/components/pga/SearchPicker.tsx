import { useState, useCallback, useRef, useEffect } from "react";
import {
  View,
  Text,
  TextInput,
  Pressable,
  FlatList,
  Modal,
  StyleSheet,
  Platform,
  SafeAreaView,
  KeyboardAvoidingView,
} from "react-native";
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
  const [open, setOpen] = useState(false);
  const displayLabelRef = useRef<string | null>(null);

  // Keep the display label in sync: update whenever items contain the selected id
  useEffect(() => {
    if (selectedId == null) {
      displayLabelRef.current = null;
      return;
    }
    const match = items.find((item) => item.id === selectedId);
    if (match) {
      displayLabelRef.current = match.label;
    }
  }, [selectedId, items]);

  const hasSelection = selectedId != null && displayLabelRef.current != null;
  const displayLabel = displayLabelRef.current;

  const handleOpen = useCallback(() => {
    onQueryChange("");
    setOpen(true);
  }, [onQueryChange]);

  const handleClose = useCallback(() => {
    onQueryChange("");
    setOpen(false);
  }, [onQueryChange]);

  const handleSelect = useCallback(
    (item: SearchItem) => {
      displayLabelRef.current = item.label;
      onSelect(item);
      onQueryChange("");
      setOpen(false);
    },
    [onSelect, onQueryChange]
  );

  const renderItem = useCallback(
    ({ item }: { item: SearchItem }) => {
      const isSelected = item.id === selectedId;
      return (
        <Pressable
          onPress={() => handleSelect(item)}
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
                {
                  color: isSelected
                    ? colors.accent.primary
                    : colors.text.primary,
                },
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
    },
    [selectedId, colors, handleSelect]
  );

  return (
    <View style={styles.wrapper}>
      <Text style={[styles.title, { color: colors.text.primary }]}>
        {title}
      </Text>

      {/* Trigger button */}
      <Pressable
        onPress={handleOpen}
        style={({ pressed }) => [
          styles.trigger,
          {
            backgroundColor: colors.surface.card,
            borderColor: hasSelection
              ? colors.accent.primary
              : colors.border.strong,
            opacity: pressed ? 0.85 : 1,
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
          style={styles.triggerIcon}
        />
        <Text
          style={[
            styles.triggerText,
            {
              color: hasSelection
                ? colors.text.primary
                : colors.text.disabled ?? colors.text.muted,
            },
          ]}
          numberOfLines={1}
        >
          {hasSelection ? displayLabel : placeholder}
        </Text>
        {hasSelection ? (
          <Ionicons
            name="checkmark-circle"
            size={18}
            color={colors.accent.primary}
            style={{ marginLeft: 8 }}
          />
        ) : (
          <Ionicons
            name="chevron-down"
            size={16}
            color={colors.text.muted}
            style={{ marginLeft: 8 }}
          />
        )}
      </Pressable>

      {helperText ? (
        <Text style={[styles.helper, { color: colors.text.muted }]}>
          {helperText}
        </Text>
      ) : null}

      {/* Modal picker */}
      <Modal
        visible={open}
        animationType="slide"
        presentationStyle="pageSheet"
        onRequestClose={handleClose}
      >
        <SafeAreaView
          style={[
            styles.modalContainer,
            { backgroundColor: colors.surface.screen },
          ]}
        >
          <KeyboardAvoidingView
            style={{ flex: 1 }}
            behavior={Platform.OS === "ios" ? "padding" : undefined}
          >
            {/* Header */}
            <View
              style={[
                styles.modalHeader,
                { borderBottomColor: colors.border.subtle },
              ]}
            >
              <Text
                style={[
                  styles.modalTitle,
                  { color: colors.text.primary },
                ]}
              >
                {title}
              </Text>
              <Pressable onPress={handleClose} hitSlop={12}>
                <Ionicons
                  name="close-circle"
                  size={28}
                  color={colors.text.muted}
                />
              </Pressable>
            </View>

            {/* Search input */}
            <View
              style={[
                styles.inputWrap,
                {
                  backgroundColor: colors.surface.card,
                  borderColor: colors.border.strong,
                  marginHorizontal: 16,
                  marginTop: 12,
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
                placeholderTextColor={
                  colors.text.disabled ?? colors.text.muted
                }
                style={[styles.input, { color: colors.text.primary }]}
                autoFocus
              />
              {query.length > 0 ? (
                <Pressable
                  onPress={() => onQueryChange("")}
                  hitSlop={8}
                >
                  <Ionicons
                    name="close-circle"
                    size={18}
                    color={colors.text.muted}
                  />
                </Pressable>
              ) : null}
            </View>

            {/* Scrollable item list */}
            <FlatList
              data={items}
              keyExtractor={(item) => String(item.id)}
              renderItem={renderItem}
              contentContainerStyle={styles.listContent}
              keyboardShouldPersistTaps="handled"
              ListEmptyComponent={
                <Text
                  style={[
                    styles.emptyText,
                    { color: colors.text.muted },
                  ]}
                >
                  {query
                    ? "No results found"
                    : "Type to search..."}
                </Text>
              }
            />
          </KeyboardAvoidingView>
        </SafeAreaView>
      </Modal>
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
  trigger: {
    flexDirection: "row",
    alignItems: "center",
    borderWidth: 1,
    borderRadius: 12,
    paddingHorizontal: 12,
    paddingVertical: 12,
  },
  triggerIcon: {
    marginRight: 8,
  },
  triggerText: {
    flex: 1,
    fontSize: 14,
    fontWeight: "500",
  },
  helper: {
    marginTop: 6,
    fontSize: 12,
    fontStyle: "italic",
  },
  modalContainer: {
    flex: 1,
  },
  modalHeader: {
    flexDirection: "row",
    alignItems: "center",
    justifyContent: "space-between",
    paddingHorizontal: 16,
    paddingVertical: 14,
    borderBottomWidth: 1,
  },
  modalTitle: {
    fontSize: 18,
    fontWeight: "700",
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
  listContent: {
    padding: 16,
    paddingBottom: 40,
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
  emptyText: {
    textAlign: "center",
    marginTop: 40,
    fontSize: 14,
  },
});
