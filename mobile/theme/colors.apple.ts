// theme/colors.apple.ts
import { ThemeColors } from "./types";

const apple: ThemeColors = {
  surface: {
    screen: "#0B1D3A",
    card: "#102A4C",
    cardSoft: "#143A63",
    elevated: "#1B4A7A",
  },

  text: {
    primary: "#FFFFFF",
    secondary: "#C7D2FE",
    muted: "#94A3B8",
    disabled: "#64748B",
    inverse: "#020617",
  },

  border: {
    subtle: "rgba(255,255,255,0.08)",
    strong: "rgba(255,255,255,0.16)",
  },

  accent: {
    primary: "#0A84FF",
    success: "#30D158",
    warning: "#FFD60A",
    danger: "#FF453A",
    info: "#64D2FF",
  },

  state: {
    active: "#0A84FF",
    hover: "#1E40AF",
    selected: "#1E3A8A",
    disabled: "#334155",
  },

  glow: {
    success: "rgba(48,209,88,0.22)",
    primary: "rgba(10,132,255,0.22)",
  },
};

export default apple;