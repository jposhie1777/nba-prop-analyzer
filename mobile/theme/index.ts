// theme/index.ts
import base from "./color";
import apple from "./colors.apple";
import fanduel from "./colors.fanduel";
import fanduelLight from "./colors.fanduelLight";

export const themes = {
  base,
  apple,
  fanduel,
  fanduelLight,
};

export type ThemeKey = keyof typeof themes;