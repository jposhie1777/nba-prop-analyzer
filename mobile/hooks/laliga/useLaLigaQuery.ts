import { useEplQuery } from "@/hooks/epl/useEplQuery";

export function useLaLigaQuery<T>(path: string, params?: Record<string, any>, enabled: boolean = true) {
  return useEplQuery<T>(path, params, enabled);
}
