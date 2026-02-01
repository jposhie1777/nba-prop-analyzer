// /app/(dev)/devStyles.ts
import { StyleSheet } from "react-native";
import { ThemeColors } from "@/theme/types";

export function createDevStyles(colors: ThemeColors) {
  return StyleSheet.create({
    screen: {
      flex: 1,
      backgroundColor: colors.surface.screen,
    },

    content: {
      padding: 14,
      gap: 12,
    },

    title: {
      fontSize: 22,
      fontWeight: "800",
      color: colors.text.primary,
    },

    section: {
      borderWidth: 1,
      borderColor: colors.border.subtle,
      borderRadius: 14,
      backgroundColor: colors.surface.card,
      overflow: "hidden",
    },

    sectionHeader: {
      paddingHorizontal: 12,
      paddingVertical: 10,
      flexDirection: "row",
      justifyContent: "space-between",
      alignItems: "center",
    },

    sectionTitle: {
      fontSize: 16,
      fontWeight: "700",
      color: colors.text.primary,
    },

    sectionChevron: {
      fontSize: 16,
      color: colors.text.muted,
    },

    sectionBody: {
      padding: 12,
      gap: 10,
      backgroundColor: colors.surface.cardSoft,
    },
    sectionSubtitle: {
      fontWeight: "700",
      color: colors.text.secondary,
    },

    kvRow: {
      flexDirection: "row",
      justifyContent: "space-between",
      gap: 10,
    },

    keyText: {
      fontWeight: "700",
      color: colors.text.secondary,
    },

    valueText: {
      flex: 1,
      textAlign: "right",
      color: colors.text.primary,
    },

    toolButton: {
      padding: 12,
      borderRadius: 12,
      borderWidth: 1,
      borderColor: colors.border.subtle,
      backgroundColor: colors.surface.card,
    },

    toolButtonDisabled: {
      opacity: 0.5,
    },

    toolTitle: {
      fontWeight: "800",
      color: colors.text.primary,
    },

    toolSubtitle: {
      marginTop: 4,
      color: colors.text.muted,
    },

    card: {
      padding: 10,
      borderRadius: 12,
      borderWidth: 1,
      borderColor: colors.border.subtle,
      backgroundColor: colors.surface.elevated,
      gap: 4,
    },

    cardTitle: {
      fontWeight: "800",
      color: colors.text.primary,
    },

    mono: {
      fontFamily: "Menlo",
      color: colors.text.secondary,
    },

    dangerText: {
      color: colors.accent.danger,
    },

    mutedText: {
      color: colors.text.muted,
    },
    codeBlock: {
      fontFamily: "Menlo",
      fontSize: 12,
      color: colors.text.muted,
      backgroundColor: colors.surface.subtle,
      padding: 8,
      borderRadius: 6,
      marginTop: 4,
    },
  });
}
