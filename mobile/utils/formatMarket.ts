export function formatMarketLabel(market: string): string {
  switch (market) {
    case "points":
    case "pts":
      return "Points";

    case "rebounds":
    case "reb":
      return "Rebounds";

    case "assists":
    case "ast":
      return "Assists";

    case "steals":
      return "Steals";

    case "blocks":
      return "Blocks";

    // 🔥 3PM — THIS IS THE IMPORTANT PART
    case "three_pointers_made":
    case "fg3m":
    case "threes":
    case "3pm":
      return "3-Pointers Made";

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
      return market
        .replace(/_/g, " ")
        .replace(/\b\w/g, (c) => c.toUpperCase());
  }
}