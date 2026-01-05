// theme/index.ts

import base from "./colors";
import { apple } from "./apple";
import { fanduel } from "./fanduel";
import { fanduelLight } from "./fanduelLight";
import { ThemeColors } from "./types";

export const themes: Record<string, ThemeColors> = {
  base,
  apple,
  fanduel,
  fanduelLight,
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