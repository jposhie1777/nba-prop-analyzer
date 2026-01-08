import { HistoricalPlayerTrend } from "@/hooks/useHistoricalPlayerTrends";

export function resolveSparkline(
  market: string,
  trend?: HistoricalPlayerTrend
) {
  if (!trend) return {};

  const m = market.toLowerCase();

  // PRA first (most specific)
  if (m.includes("pra")) {
    return {
      l5: trend.pra_last5_list,
      l10: trend.pra_last10_list,
      l20: trend.pra_last20_list,
    };
  }

  if (m.includes("points") || m.includes("pts")) {
    return {
      l5: trend.pts_last5_list,
      l10: trend.pts_last10_list,
      l20: trend.pts_last20_list,
    };
  }

  if (m.includes("assists") || m.includes("ast")) {
    return {
      l5: trend.ast_last5_list,
      l10: trend.ast_last10_list,
      l20: trend.ast_last20_list,
    };
  }

  if (m.includes("rebounds") || m.includes("reb")) {
    return {
      l5: trend.reb_last5_list,
      l10: trend.reb_last10_list,
      l20: trend.reb_last20_list,
    };
  }

  // safe default
  return {
    l5: trend.pts_last5_list,
    l10: trend.pts_last10_list,
    l20: trend.pts_last20_list,
  };
}