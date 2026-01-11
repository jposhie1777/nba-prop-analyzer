export function formatMarketLabel(market: string): string {
  switch (market) {
    case "points":
      return "Points";
    case "rebounds":
      return "Rebounds";
    case "assists":
      return "Assists";
    case "steals":
      return "Steals";
    case "blocks":
      return "Blocks";
    case "threes":
      return "3-Pointer Made";

    case "points_rebounds":
      return "Points + Rebounds";
    case "points_assists":
      return "Points + Assists";
    case "rebounds_assists":
      return "Rebounds + Assists";
    case "points_rebounds_assists":
      return "Pts + Reb + Ast";

    case "double_double":
      return "Double Double";
    case "triple_double":
      return "Triple Double";

    default:
      // Fallback: title-case unknown markets safely
      return market
        .replace(/_/g, " ")
        .replace(/\b\w/g, (c) => c.toUpperCase());
  }
}