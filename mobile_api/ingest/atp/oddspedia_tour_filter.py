from __future__ import annotations

import re
from collections import Counter
from typing import Any, Dict, Iterable, List, Optional, Set, Tuple

_WTA_MARKERS = (
    "wta",
    "women",
    "girls",
    "itf-women",
)

_ATP_MARKERS = (
    "atp",
    "men",
    "boys",
    "challenger",
    "itf-men",
    "davis cup",
    "laver cup",
)

_META_KEYS = (
    "league",
    "tour",
    "tournament",
    "category",
    "competition",
    "season",
    "gender",
    "sex",
    "slug",
    "name",
    "title",
    "url",
    "path",
)

_SEASON_ID_KEYS = ("season_id", "seasonid")
_SLUG_KEYS = ("slug", "league_slug", "tournament_slug", "category_slug")


def _normalize_text(value: str) -> str:
    return re.sub(r"[^a-z0-9]+", " ", str(value).lower()).strip()


def _key_is_meta(key: str) -> bool:
    lk = key.lower()
    return any(token in lk for token in _META_KEYS)


def _collect_meta_strings(node: Any, *, depth: int = 0, max_depth: int = 4) -> List[str]:
    if depth > max_depth:
        return []
    out: List[str] = []
    if isinstance(node, str):
        text = _normalize_text(node)
        if text:
            out.append(text)
        return out
    if isinstance(node, dict):
        for key, value in node.items():
            if _key_is_meta(str(key)):
                out.extend(_collect_meta_strings(value, depth=depth + 1, max_depth=max_depth))
            elif isinstance(value, dict) and depth < max_depth:
                out.extend(_collect_meta_strings(value, depth=depth + 1, max_depth=max_depth))
        return out
    if isinstance(node, list):
        for item in node:
            out.extend(_collect_meta_strings(item, depth=depth + 1, max_depth=max_depth))
    return out


def _collect_season_ids(node: Any, *, depth: int = 0, max_depth: int = 4) -> Set[int]:
    if depth > max_depth:
        return set()
    out: Set[int] = set()
    if isinstance(node, dict):
        for key, value in node.items():
            lk = str(key).lower()
            if lk in _SEASON_ID_KEYS:
                try:
                    out.add(int(value))
                except (TypeError, ValueError):
                    pass
            if isinstance(value, (dict, list)):
                out.update(_collect_season_ids(value, depth=depth + 1, max_depth=max_depth))
    elif isinstance(node, list):
        for item in node:
            out.update(_collect_season_ids(item, depth=depth + 1, max_depth=max_depth))
    return out


def _collect_slugs(node: Any, *, depth: int = 0, max_depth: int = 4) -> Set[str]:
    if depth > max_depth:
        return set()
    out: Set[str] = set()
    if isinstance(node, dict):
        for key, value in node.items():
            lk = str(key).lower()
            if lk in _SLUG_KEYS and isinstance(value, str):
                text = _normalize_text(value)
                if text:
                    out.add(text)
            if isinstance(value, (dict, list)):
                out.update(_collect_slugs(value, depth=depth + 1, max_depth=max_depth))
    elif isinstance(node, list):
        for item in node:
            out.update(_collect_slugs(item, depth=depth + 1, max_depth=max_depth))
    return out


def _has_any_marker(text_blobs: Iterable[str], markers: Tuple[str, ...]) -> bool:
    merged = " ".join(text_blobs)
    return any(_normalize_text(marker) in merged for marker in markers)


def _is_target_slug(slugs: Set[str], target_slug: Optional[str]) -> bool:
    if not target_slug:
        return True
    t = _normalize_text(target_slug)
    if not t:
        return True
    return any(t == slug or t in slug for slug in slugs)


def _is_atp_match(
    match: Dict[str, Any],
    *,
    target_league_slug: Optional[str],
    target_season_id: Optional[int],
) -> Tuple[bool, str]:
    info = match.get("match_info") if isinstance(match.get("match_info"), dict) else {}
    text_sources = _collect_meta_strings(match) + _collect_meta_strings(info)
    slugs = _collect_slugs(match) | _collect_slugs(info)
    season_ids = _collect_season_ids(match) | _collect_season_ids(info)

    if _has_any_marker(text_sources, _WTA_MARKERS):
        return False, "wta_marker"

    if target_league_slug and slugs and not _is_target_slug(slugs, target_league_slug):
        return False, "league_slug_mismatch"

    if target_season_id is not None and season_ids and target_season_id not in season_ids:
        return False, "season_id_mismatch"

    if _has_any_marker(text_sources, _ATP_MARKERS):
        return True, "atp_marker"

    return True, "no_negative_marker"


def filter_atp_matches(
    matches: List[Dict[str, Any]],
    *,
    target_league_slug: Optional[str],
    target_season_id: Optional[int],
    log_prefix: str,
) -> List[Dict[str, Any]]:
    kept: List[Dict[str, Any]] = []
    dropped = Counter()

    for match in matches:
        keep, reason = _is_atp_match(
            match,
            target_league_slug=target_league_slug,
            target_season_id=target_season_id,
        )
        if keep:
            kept.append(match)
        else:
            dropped[reason] += 1

    if dropped:
        bits = ", ".join(f"{k}={v}" for k, v in dropped.most_common())
        print(
            f"{log_prefix} ATP-only filter removed {sum(dropped.values())} matches "
            f"({bits})"
        )
    print(f"{log_prefix} ATP-only filter kept {len(kept)}/{len(matches)} matches")
    return kept
