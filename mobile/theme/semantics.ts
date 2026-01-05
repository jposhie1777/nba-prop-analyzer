// theme/semantics.ts
import colors from "./colors";

export const confidenceColor = (v: number) => {
  if (v >= 80) return colors.accent.success;
  if (v >= 65) return colors.accent.warning;
  return colors.text.muted;
};

export const confidenceAccent = (v: number) => {
  if (v >= 80) return colors.accent.success;
  if (v >= 65) return colors.accent.warning;
  return colors.border.strong;
};