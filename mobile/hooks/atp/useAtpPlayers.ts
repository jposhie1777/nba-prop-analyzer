import { useEffect, useState } from "react";
import { useAtpQuery } from "./useAtpQuery";
import { AtpPlayer } from "@/types/atp";

type Response = {
  data: AtpPlayer[];
  count: number;
};

function useDebouncedValue<T>(value: T, delay: number): T {
  const [debounced, setDebounced] = useState(value);
  useEffect(() => {
    const timer = setTimeout(() => setDebounced(value), delay);
    return () => clearTimeout(timer);
  }, [value, delay]);
  return debounced;
}

export function useAtpPlayers(params?: { search?: string }) {
  const debouncedSearch = useDebouncedValue(params?.search ?? "", 350);
  return useAtpQuery<Response>("/atp/players", {
    search: debouncedSearch || undefined,
    per_page: 50,
  });
}
