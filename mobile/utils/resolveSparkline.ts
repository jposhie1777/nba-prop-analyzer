import { HistoricalPlayerTrend } from "@/hooks/useHistoricalPlayerTrends";

type SparklineResult = {
  sparkline_l5?: number[];
  sparkline_l10?: number[];
  sparkline_l20?: number[];
};

export function resolveSparklineByMarket(
  marketKey: string,
  trend?: HistoricalPlayerTrend
): SparklineResult {
  if (!trend || !marketKey) return {};

  switch (marketKey) {
    /* ======================
       CORE STATS
    ====================== */
    case "pts":
      return {
        sparkline_l5: trend.pts_last5_list,
        sparkline_l10: trend.pts_last10_list,
        sparkline_l20: trend.pts_last20_list,
      };

    case "reb":
      return {
        sparkline_l5: trend.reb_last5_list,
        sparkline_l10: trend.reb_last10_list,
        sparkline_l20: trend.reb_last20_list,
      };

    case "ast":
      return {
        sparkline_l5: trend.ast_last5_list,
        sparkline_l10: trend.ast_last10_list,
        sparkline_l20: trend.ast_last20_list,
      };

    case "stl":
      return {
        sparkline_l5: trend.stl_last5_list,
        sparkline_l10: trend.stl_last10_list,
        sparkline_l20: trend.stl_last20_list,
      };

    case "blk":
      return {
        sparkline_l5: trend.blk_last5_list,
        sparkline_l10: trend.blk_last10_list,
        sparkline_l20: trend.blk_last20_list,
      };

    case "3pm":
      return {
        sparkline_l5: trend.fg3m_last5_list,
        sparkline_l10: trend.fg3m_last10_list,
        sparkline_l20: trend.fg3m_last20_list,
      };

    /* ======================
       COMBO MARKETS
    ====================== */
    case "pr":
      return {
        sparkline_l5: trend.pr_last5_list,
        sparkline_l10: trend.pr_last10_list,
        sparkline_l20: trend.pr_last20_list,
      };

    case "pa":
      return {
        sparkline_l5: trend.pa_last5_list,
        sparkline_l10: trend.pa_last10_list,
        sparkline_l20: trend.pa_last20_list,
      };

    case "ra":
      return {
        sparkline_l5: trend.ra_last5_list,
        sparkline_l10: trend.ra_last10_list,
        sparkline_l20: trend.ra_last20_list,
      };

    case "pra":
      return {
        sparkline_l5: trend.pra_last5_list,
        sparkline_l10: trend.pra_last10_list,
        sparkline_l20: trend.pra_last20_list,
      };

    /* ======================
       MILESTONES
    ====================== */
    case "dd":
      return {
        sparkline_l5: trend.dd_last5_list,
        sparkline_l10: trend.dd_last10_list,
        sparkline_l20: trend.dd_last20_list,
      };

    case "td":
      return {
        sparkline_l5: trend.td_last5_list,
        sparkline_l10: trend.td_last10_list,
        sparkline_l20: trend.td_last20_list,
      };

    /* ======================
       FALLBACK
    ====================== */
    default:
      if (__DEV__) {
        console.warn("Unhandled sparkline market_key:", marketKey);
      }
      return {};
  }
}
