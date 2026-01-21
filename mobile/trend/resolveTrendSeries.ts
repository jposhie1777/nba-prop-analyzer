import type { HistoricalPlayerTrend } from "@/hooks/useHistoricalPlayerTrends";

export type TrendWindow = 5 | 10 | 20;

export type TrendSeries = {
  values: number[];
  dates: string[];
};

export function resolveTrendSeries(
  trend: HistoricalPlayerTrend | undefined,
  market: string,
  window: TrendWindow
): TrendSeries {
  if (!trend) return { values: [], dates: [] };

  const key = market.toLowerCase();

  const values =
    window === 5
      ? (trend[`${key}_last5_list` as keyof HistoricalPlayerTrend] as number[])
      : window === 10
      ? (trend[`${key}_last10_list`] as number[])
      : (trend[`${key}_last20_list`] as number[]);

  const dates =
    window === 5
      ? trend.last5_dates
      : window === 10
      ? trend.last10_dates
      : trend.last20_dates;

  return {
    values: values ?? [],
    dates: dates ?? [],
  };
}