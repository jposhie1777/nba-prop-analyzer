// hooks/useSortableData.ts
import { useMemo, useState } from "react";

type Direction = "asc" | "desc";

export function useSortableData<T>(
  data: T[],
  defaultKey: keyof T
) {
  const [sortKey, setSortKey] = useState<keyof T>(defaultKey);
  const [direction, setDirection] = useState<Direction>("desc");

  const sortedData = useMemo(() => {
    return [...data].sort((a, b) => {
      const aVal = a[sortKey];
      const bVal = b[sortKey];

      if (aVal == null || bVal == null) return 0;

      if (aVal < bVal) return direction === "asc" ? -1 : 1;
      if (aVal > bVal) return direction === "asc" ? 1 : -1;
      return 0;
    });
  }, [data, sortKey, direction]);

  const toggleSort = (key: keyof T) => {
    if (key === sortKey) {
      setDirection(d => (d === "asc" ? "desc" : "asc"));
    } else {
      setSortKey(key);
      setDirection("desc");
    }
  };

  return { sortedData, sortKey, direction, toggleSort };
}