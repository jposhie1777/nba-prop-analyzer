import { StatMarket } from "@/store/useParlayTracker";

export function normalizeMarket(mkt: string): StatMarket {
  const key = mkt
    .toLowerCase()
    .replace(/\s+/g, "")
    .replace(/_/g, "");
  const altMarketMap: Record<string, string> = {
    playerpoints: "pts",
    playerpointsalternate: "pts",
    pointsalternate: "pts",
    playerrebounds: "reb",
    playerreboundsalternate: "reb",
    reboundsalternate: "reb",
    playerassists: "ast",
    playerassistsalternate: "ast",
    assistsalternate: "ast",
    playerthrees: "fg3m",
    playerthreesalternate: "fg3m",
    threesalternate: "fg3m",
    playerpointsreboundsassists: "pra",
    playerpointsreboundsassistsalternate: "pra",
    pointsreboundsassistsalternate: "pra",
    playerpointsrebounds: "pr",
    playerpointsreboundsalternate: "pr",
    pointsreboundsalternate: "pr",
    playerpointsassists: "pa",
    playerpointsassistsalternate: "pa",
    pointsassistsalternate: "pa",
    playerreboundsassists: "ra",
    playerreboundsassistsalternate: "ra",
    reboundsassistsalternate: "ra",
  };
  const alt = altMarketMap[key];
  if (alt) return alt as StatMarket;
  if (
    key === "3pm" ||
    key === "3pt" ||
    key === "3pts" ||
    key === "threes" ||
    key === "threepointersmade" ||
    key === "three_pointers_made" ||
    key === "fg3m"
  ) {
    return "fg3m";
  }
  if (key === "pts" || key === "point" || key === "points") return "pts";
  if (key === "reb" || key === "rebound" || key === "rebounds") return "reb";
  if (key === "ast" || key === "assist" || key === "assists") return "ast";
  return key as StatMarket;
}
