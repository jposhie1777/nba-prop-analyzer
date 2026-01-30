// mobile/theme/color.ts

const colors = {
  /* ======================
     SURFACES
  ====================== */
  surface: {
    screen: "#0E1117",      // app background
    card: "#151A21",        // primary cards
    cardSoft: "#1B222C",    // expanded / inner sections
    elevated: "#202938",    // modals, popovers
  },

  /* ======================
     TEXT
  ====================== */
  text: {
    primary: "#E5E7EB",
    secondary: "#94A3B8",
    muted: "#64748B",
    disabled: "#475569",
    inverse: "#0F172A",
  },

  /* ======================
     BORDERS / DIVIDERS
  ====================== */
  border: {
    subtle: "rgba(255,255,255,0.06)",
    strong: "rgba(255,255,255,0.12)",
  },

  /* ======================
     ACCENTS
  ====================== */
  accent: {
    primary: "#6366F1",   // active tabs, toggles
    success: "#22C55E",   // confidence / hit
    warning: "#F59E0B",   // medium confidence
    danger: "#EF4444",    // bad / risk
    info: "#60A5FA",
  },

  /* ======================
     INTERACTIVE STATES
  ====================== */
  state: {
    active: "#6366F1",
    hover: "#1F2937",
    selected: "#312E81",
    disabled: "#334155",
  },

  /* ======================
     SPECIAL PURPOSE
  ====================== */
  glow: {
    success: "rgba(34,197,94,0.18)",
    primary: "rgba(99,102,241,0.18)",
  },
};

export default colors;
