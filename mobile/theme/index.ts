// theme/index.ts

import base from "./color";
import apple from "./colors.apple";
import fanduel from "./colors.fanduel";
import fanduelLight from "./colors.fanduelLight";

import type { ThemeColors } from "./types";

/* ======================================================
   THEMES MAP
====================================================== */

export const themes: Record<string, ThemeColors> = {
  base,
  apple,
  fanduel,
  fanduelLight,
};

/* ======================================================
   DEFAULT EXPORT
====================================================== */

export default themes;