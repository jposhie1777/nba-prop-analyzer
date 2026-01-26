// mobile/utils/formatMarket.ts
export function formatMarketLabel(market?: string): string {
  if (!market) return "";

  switch (market) {
    /* ---------- CORE ---------- */
    case "pts":
      return "Points";

    case "reb":
      return "Rebounds";

    case "ast":
      return "Assists";

    case "stl":
      return "Steals";

    case "blk":
      return "Blocks";

    case "tov":
      return "Turnovers";

    /* ---------- THREES ---------- */
    case "3pm":
      return "3-Pointers Made";

    /* ---------- COMBOS ---------- */
    case "pr":
      return "Points + Rebounds";

    case "pa":
      return "Points + Assists";

    case "ra":
      return "Rebounds + Assists";

    case "pra":
      return "Points + Rebounds + Assists";

    /* ---------- SPECIALS ---------- */
    case "dd":
      return "Double Double";

    case "td":
      return "Triple Double";

    /* ---------- FALLBACK (DEV SAFETY) ---------- */
    default:
      return market
        .replace(/_/g, " ")
        .replace(/\b\w/g, (c) => c.toUpperCase());
  }
}
