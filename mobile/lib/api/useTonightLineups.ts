import { useEffect, useState } from "react";
import { API_URL } from "@/lib/config";

export function useTonightLineups() {
  const [data, setData] = useState<any[]>([]);
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    fetch(`${API_URL}/lineups/tonight`)
      .then(res => res.json())
      .then(setData)
      .finally(() => setLoading(false));
  }, []);

  return { data, loading };
}