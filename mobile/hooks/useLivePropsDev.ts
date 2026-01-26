import { useQuery } from "@tanstack/react-query";
import { fetchLivePropsDev } from "@/lib/apiLivePropsDev";

export function useLivePropsDev(limit = 100) {
  return useQuery({
    queryKey: ["live-props-dev"],
    queryFn: () => fetchLivePropsDev(limit),
  });
}
