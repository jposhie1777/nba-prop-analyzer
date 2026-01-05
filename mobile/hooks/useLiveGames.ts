import { useEffect, useRef } from "react";

export function useLiveGames() {
  const intervalRef = useRef<ReturnType<typeof setInterval> | null>(null);

  const fetchGames = async () => {
    // fetch logic
  };

  useEffect(() => {
    fetchGames();

    intervalRef.current = setInterval(fetchGames, 15000);

    return () => {
      if (intervalRef.current) {
        clearInterval(intervalRef.current);
      }
    };
  }, []);

  return {};
}
