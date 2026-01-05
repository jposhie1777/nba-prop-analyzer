// theme/apple.ts
import { ThemeColors } from "./types";

export const apple: ThemeColors = {
  surface: {
    screen: "#0A1A33",
    card: "#0E223F",
    cardSoft: "#102A4F",
    elevated: "#14325C",
  },

  text: {
    primary: "#FFFFFF",
    secondary: "#C7D2E0",
    muted: "#8CA3C0",
    disabled: "#64748B",
    inverse: "#0A1A33",
  },

  border: {
    subtle: "rgba(255,255,255,0.08)",
    strong: "rgba(255,255,255,0.16)",
  },

  accent: {
    primary: "#4DA3FF",
    success: "#34D399",
    warning: "#FBBF24",
    danger: "#EF4444",
    info: "#60A5FA",
  },

  state: {
    active: "#4DA3FF",
    hover: "#0F2A4F",
    selected: "#163A6B",
    disabled: "#334155",
  },

  glow: {
    success: "rgba(52,211,153,0.18)",
    primary: "rgba(77,163,255,0.18)",
  },
};