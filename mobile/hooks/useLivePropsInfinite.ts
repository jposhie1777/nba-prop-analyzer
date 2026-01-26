import { useInfiniteQuery } from "@tanstack/react-query";
import { fetchLiveProps } from "@/lib/apiLive";

/* reuse helpers from useLivePropsDev */
import {
  periodNumToLabel,
  secondsToClock,
  resolveCurrentStat,
  resolveDisplayOdds,
} from "./livePropHelpers"; // or inline if you prefer

export function useLivePropsInfinite() {
  return useInfiniteQuery({
    queryKey: ["live-props"],
    queryFn: ({ pageParam }) =>
      fetchLiveProps({
        limit: 100,
        cursor: pageParam,
      }),

    getNextPageParam: (lastPage) =>
      lastPage.next_cursor ?? undefined,

    refetchInterval: 15_000,
    staleTime: 5_000,
  });
}