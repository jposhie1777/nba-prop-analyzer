// theme/index.ts

import base from "./color";               // default export ✔
import apple from "./colors.apple";
import fanduel from "./colors.fanduel";
import fanduelLight from "./colors.fanduelLight";

/**
 * All available themes
 */
export const themes = {
  base,          // existing default theme
  apple,         // Apple Sports
  fanduel,       // FanDuel Dark
  fanduelLight,  // FanDuel Light
};

/**
 * Theme name union
 */
export type ThemeName = keyof typeof themes;

/**
 * Theme shape (typed off base)
 */
export type Theme = typeof base;

/**
 * Backwards compatibility:
 * Allows `import colors from "@/theme"`
 */
export default themes;