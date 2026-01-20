// mobile/store/useTheme.ts
import { create } from "zustand";
import { themes, ThemeName } from "@/theme";

type ThemeState = {
  theme: ThemeName;
  colors: typeof themes.base;
  setTheme: (theme: ThemeName) => void;
};

export const useTheme = create<ThemeState>((set) => ({
  theme: "base",
  colors: themes.base,

  setTheme: (theme) =>
    set({
      theme,
      colors: themes[theme],
    }),
}));