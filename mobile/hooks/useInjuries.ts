// hooks/useInjuries.ts
import { useCallback, useEffect, useState } from "react";
import {
  fetchInjuries,
  InjuriesResponse,
  InjuryRecord,
  TeamInjuries,
} from "@/lib/injuries";

type UseInjuriesOptions = {
  team?: string;
  status?: string;
};

export function useInjuries(options: UseInjuriesOptions = {}) {
  const [data, setData] = useState<InjuriesResponse | null>(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);

  const load = useCallback(async () => {
    setLoading(true);
    setError(null);
    try {
      const response = await fetchInjuries({
        team: options.team,
        status: options.status,
      });
      setData(response);
    } catch (err) {
      setError(err instanceof Error ? err.message : "Unknown error");
    } finally {
      setLoading(false);
    }
  }, [options.team, options.status]);

  useEffect(() => {
    load();
  }, [load]);

  return {
    data,
    injuries: data?.injuries ?? [],
    byTeam: data?.by_team ?? [],
    statusSummary: data?.status_summary ?? null,
    count: data?.count ?? 0,
    loading,
    error,
    refresh: load,
  };
}

// Hook for team-specific injuries
export function useTeamInjuries(teamAbbr: string | null) {
  const [data, setData] = useState<InjuryRecord[]>([]);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);

  const load = useCallback(async () => {
    if (!teamAbbr) {
      setData([]);
      return;
    }

    setLoading(true);
    setError(null);
    try {
      const response = await fetchInjuries({ team: teamAbbr });
      setData(response.injuries);
    } catch (err) {
      setError(err instanceof Error ? err.message : "Unknown error");
    } finally {
      setLoading(false);
    }
  }, [teamAbbr]);

  useEffect(() => {
    load();
  }, [load]);

  return {
    injuries: data,
    loading,
    error,
    refresh: load,
  };
}
