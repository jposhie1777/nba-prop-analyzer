import { StatMarket } from "@/store/useParlayTracker";

export function normalizeMarket(mkt: string): StatMarket {
  const key = mkt.toLowerCase();
  if (key === "3pm" || key === "threes" || key === "three_pointers_made") {
    return "fg3m";
  }
  return key as StatMarket;
}
