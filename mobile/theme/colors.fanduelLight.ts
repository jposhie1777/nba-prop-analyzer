// theme/fanduelLight.ts
import { ThemeColors } from "./types";

export const fanduelLight: ThemeColors = {
  surface: {
    screen: "#F5F7FA",
    card: "#FFFFFF",
    cardSoft: "#EEF2F7",
    elevated: "#E2E8F0",
  },

  text: {
    primary: "#0F172A",
    secondary: "#334155",
    muted: "#64748B",
    disabled: "#94A3B8",
    inverse: "#FFFFFF",
  },

  border: {
    subtle: "rgba(15,23,42,0.06)",
    strong: "rgba(15,23,42,0.12)",
  },

  accent: {
    primary: "#4F46E5",
    success: "#059669",
    warning: "#D97706",
    danger: "#DC2626",
    info: "#0284C7",
  },

  state: {
    active: "#4F46E5",
    hover: "#EEF2FF",
    selected: "#E0E7FF",
    disabled: "#CBD5E1",
  },

  glow: {
    success: "rgba(5,150,105,0.14)",
    primary: "rgba(79,70,229,0.14)",
  },
};

export default fanduelLight;
