from __future__ import annotations

import json
import os
import re
from dataclasses import dataclass
from typing import Any, Dict, List, Tuple


@dataclass(frozen=True)
class SelectionResult:
    included: List[Dict[str, Any]]
    deep_dives: List[Dict[str, Any]]
    stats: Dict[str, Any]

    # Backward-compat convenience (legacy attribute name).
    @property
    def selected(self) -> List[Dict[str, Any]]:  # pragma: no cover - compatibility shim
        return self.included


def load_cybermed_selection_config(path: str = "data/cybermed_selection.json") -> Dict[str, Any]:
    """
    Loads Cybermed selection policy config from JSON.

    Safe defaults:
      - If config file is missing or invalid, selection is disabled (pass-through).
    """
    if not path:
        return {"enabled": False}

    try:
        with open(path, "r", encoding="utf-8") as f:
            cfg = json.load(f)
        if not isinstance(cfg, dict):
            return {"enabled": False}
        return cfg
    except FileNotFoundError:
        return {"enabled": False}
    except Exception:
        return {"enabled": False}


def _norm(s: str) -> str:
    return (s or "").strip().lower()


def _normalize_journal_token(s: str) -> str:
    base = (s or "").strip().lower()
    if not base:
        return ""
    return re.sub(r"[^a-z0-9]+", "", base)


def _journal_candidates(item: Dict[str, Any]) -> List[str]:
    candidates: List[str] = []
    for key in ("journal", "journal_iso_abbrev", "journal_medline_ta"):
        val = str(item.get(key) or "").strip()
        if val:
            candidates.append(val)

    ch = str(item.get("channel") or "").strip()
    if ch:
        if ch.lower().startswith("pubmed:"):
            ch = ch.split(":", 1)[1].strip()
        if ch:
            candidates.append(ch)

    return candidates


def _journal_matches(item: Dict[str, Any], names: List[str] | set[str]) -> bool:
    normalized_targets = {
        _normalize_journal_token(str(x)) for x in names if _normalize_journal_token(str(x))
    }
    if not normalized_targets:
        return False

    for cand in _journal_candidates(item):
        if _normalize_journal_token(cand) in normalized_targets:
            return True

    return False


def _text_haystack(item: Dict[str, Any]) -> str:
    # Keep bounded to avoid large prompts/logging overhead; selector only needs signals.
    title = str(item.get("title") or "")
    journal = str(item.get("journal") or item.get("channel") or "")
    text = str(item.get("text") or "")
    return f"{title}\n{journal}\n{text[:2000]}".lower()


def _matches_any_regex(text: str, patterns: List[str]) -> bool:
    for p in patterns:
        try:
            if re.search(p, text, flags=re.IGNORECASE):
                return True
        except re.error:
            # Ignore invalid regex patterns (config safety)
            continue
    return False


def _contains_any_keyword(text: str, keywords: List[str]) -> bool:
    t = text.lower()
    for k in keywords:
        kk = (k or "").strip().lower()
        if not kk:
            continue
        if kk in t:
            return True
    return False


def _journal_name(item: Dict[str, Any]) -> str:
    candidates = _journal_candidates(item)
    return candidates[0] if candidates else ""


def _score_item(item: Dict[str, Any], cfg: Dict[str, Any]) -> Tuple[float, List[str]]:
    sel = cfg.get("selection", {}) if isinstance(cfg.get("selection"), dict) else {}
    sc = cfg.get("scoring", {}) if isinstance(cfg.get("scoring"), dict) else {}
    kw = cfg.get("classification_keywords", {}) if isinstance(cfg.get("classification_keywords"), dict) else {}

    reasons: List[str] = []
    score = 0.0

    title = str(item.get("title") or "")
    hay = _text_haystack(item)

    exclude_patterns = sel.get("exclude_title_regex", [])
    if isinstance(exclude_patterns, list) and _matches_any_regex(title, [str(x) for x in exclude_patterns]):
        penalty = float(sc.get("exclude_title_penalty", -5.0))
        score += penalty
        reasons.append(f"exclude_title_penalty({penalty})")

    # Abstract length heuristic
    min_abs_chars = int(sel.get("min_abstract_chars", 0) or 0)
    text = str(item.get("text") or "")
    if min_abs_chars > 0 and len(text.strip()) >= min_abs_chars:
        bonus = float(sc.get("has_reasonable_abstract_bonus", 1.0))
        score += bonus
        reasons.append(f"abstract_len_bonus(+{bonus})")

    # Journal bonuses (safe, offline)
    core = sel.get("core_journals", [])
    high = sel.get("high_impact_journals", [])
    core_set = {str(x).strip() for x in core} if isinstance(core, list) else set()
    high_set = {str(x).strip() for x in high} if isinstance(high, list) else set()

    if _journal_matches(item, core_set):
        bonus = float(sc.get("journal_core_bonus", 2.0))
        score += bonus
        reasons.append(f"core_journal(+{bonus})")

    if _journal_matches(item, high_set):
        bonus = float(sc.get("journal_high_impact_bonus", 2.0))
        score += bonus
        reasons.append(f"high_impact_journal(+{bonus})")

    # Track bonuses (lightweight clinical relevance signal)
    cc_kw = kw.get("critical_care", []) if isinstance(kw.get("critical_care"), list) else []
    an_kw = kw.get("anaesthesiology", []) if isinstance(kw.get("anaesthesiology"), list) else []

    if _contains_any_keyword(hay, [str(x) for x in cc_kw]):
        bonus = float(sc.get("critical_care_bonus", 0.5))
        score += bonus
        reasons.append(f"critical_care_signal(+{bonus})")

    if _contains_any_keyword(hay, [str(x) for x in an_kw]):
        bonus = float(sc.get("anaesthesiology_bonus", 0.5))
        score += bonus
        reasons.append(f"anaesthesiology_signal(+{bonus})")

    return score, reasons


def select_cybermed_pubmed_items(
    pubmed_items: List[Dict[str, Any]],
    *,
    config_path: str | None = None,
) -> SelectionResult:
    """
    Applies Cybermed selection policy to PubMed items.

    Input expectations:
      - pubmed_items are already "new" items (i.e., state-filtered in main).
      - each item may contain: title, journal, channel, text(abstract), year, url, etc.

    Output:
      - selected list (subset, ordered by score desc)
      - stats dict (counts + non-sensitive diagnostics)
    """
    path = config_path or os.getenv("CYBERMED_SELECTION_CONFIG", "data/cybermed_selection.json")
    cfg = load_cybermed_selection_config(path)

    # If disabled, pass-through to preserve current behavior.
    if not bool(cfg.get("enabled", False)):
        return SelectionResult(
            included=list(pubmed_items),
            deep_dives=list(pubmed_items),
            stats={
                "enabled": False,
                "candidates": len(pubmed_items),
                "included": len(pubmed_items),
                "deep_dives": len(pubmed_items),
                "config_path": path,
            },
        )

    sel = cfg.get("selection", {}) if isinstance(cfg.get("selection"), dict) else {}

    max_overview = int(sel.get("max_overview_items", sel.get("max_selected_per_run", 12)) or 12)
    max_deep_dives = int(sel.get("max_deep_dives", 8) or 8)
    min_score = float(sel.get("min_score_to_select", 2.0))

    journal_mode = str(sel.get("journal_allowlist_mode", "prefer") or "prefer").strip().lower()
    core = sel.get("core_journals", [])
    core_set = {str(x).strip() for x in core} if isinstance(core, list) else set()

    scored: List[Tuple[float, Dict[str, Any], List[str]]] = []
    excluded_by_allowlist = 0

    for it in pubmed_items:
        score, reasons = _score_item(it, cfg)

        # Optional strict allowlist mode (not recommended by default)
        if journal_mode == "strict":
            if core_set and not _journal_matches(it, core_set):
                excluded_by_allowlist += 1
                continue

        scored.append((score, it, reasons))

    scored.sort(key=lambda x: x[0], reverse=True)

    included: List[Dict[str, Any]] = []
    deep_dives: List[Dict[str, Any]] = []
    below_threshold = 0

    for score, it, reasons in scored:
        if score < min_score:
            below_threshold += 1
            continue

        if len(included) >= max_overview:
            break

        enriched = dict(it)
        rank = len(included) + 1
        enriched["cybermed_score"] = score
        enriched["cybermed_rank"] = rank
        enriched["cybermed_included"] = True
        enriched["cybermed_deep_dive"] = len(deep_dives) < max_deep_dives
        enriched["cybermed_selection_reasons"] = list(reasons)

        included.append(enriched)
        if enriched["cybermed_deep_dive"]:
            deep_dives.append(enriched)

    # Non-sensitive stats only (no titles, no URLs, no recipients).
    stats = {
        "enabled": True,
        "config_path": path,
        "journal_allowlist_mode": journal_mode,
        "candidates": len(pubmed_items),
        "excluded_by_allowlist": excluded_by_allowlist,
        "below_threshold": below_threshold,
        "included": len(included),
        "deep_dives": len(deep_dives),
        "selected": len(included),
        "min_score": min_score,
        "max_overview_items": max_overview,
        "max_deep_dives": max_deep_dives,
        "top_scores": [round(s[0], 2) for s in scored[: min(5, len(scored))]],
    }

    return SelectionResult(included=included, deep_dives=deep_dives, stats=stats)

