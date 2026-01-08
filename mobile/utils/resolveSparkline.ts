// utils/resolveSparkline.ts
import { HistoricalPlayerTrend } from "@/hooks/useHistoricalPlayerTrends";

export function resolveSparklineByMarket(
  market: string,
  trend?: HistoricalPlayerTrend
) {
  if (!trend) return {};

  const m = market.toLowerCase();

  if (m.includes("pra")) {
    return {
      sparkline_l5: trend.pra_last5_list,
      sparkline_l10: trend.pra_last10_list,
      sparkline_l20: trend.pra_last20_list,
    };
  }

  if (m.includes("ast")) {
    return {
      sparkline_l5: trend.ast_last5_list,
      sparkline_l10: trend.ast_last10_list,
      sparkline_l20: trend.ast_last20_list,
    };
  }

  if (m.includes("reb")) {
    return {
      sparkline_l5: trend.reb_last5_list,
      sparkline_l10: trend.reb_last10_list,
      sparkline_l20: trend.reb_last20_list,
    };
  }

  // default → points
  return {
    sparkline_l5: trend.pts_last5_list,
    sparkline_l10: trend.pts_last10_list,
    sparkline_l20: trend.pts_last20_list,
  };
}
