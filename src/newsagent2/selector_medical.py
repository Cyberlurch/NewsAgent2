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


def _journal_tier(item: Dict[str, Any], tiers: Dict[str, Any]) -> str:
    if not isinstance(tiers, dict):
        return ""

    for tier_name, journals in tiers.items():
        if isinstance(journals, list) and _journal_matches(item, journals):
            return str(tier_name)

    return ""


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


def _domain_signals(hay: str, cfg: Dict[str, Any]) -> Tuple[bool, Dict[str, bool]]:
    if not isinstance(cfg, dict):
        return False, {}

    flags: Dict[str, bool] = {}
    for name, kws in cfg.items():
        flags[name] = _contains_any_keyword(hay, [str(x) for x in kws]) if isinstance(kws, list) else False

    return any(flags.values()), flags


def _clinical_intent(hay: str, cfg: Dict[str, Any]) -> Tuple[bool, Dict[str, bool]]:
    if not isinstance(cfg, dict):
        return False, {}

    design_kw = cfg.get("design", []) if isinstance(cfg.get("design"), list) else []
    clinical_kw = cfg.get("clinical", []) if isinstance(cfg.get("clinical"), list) else []

    design = _contains_any_keyword(hay, [str(x) for x in design_kw])
    clinical = _contains_any_keyword(hay, [str(x) for x in clinical_kw])

    return design or clinical, {"design": design, "clinical": clinical}


def _pain_scope(
    hay: str,
    sel_cfg: Dict[str, Any],
    domain_flags: Dict[str, bool],
    clinical_intent: bool,
) -> Tuple[bool, Dict[str, bool]]:
    pain_kw = sel_cfg.get("pain_strict_keywords", []) if isinstance(sel_cfg.get("pain_strict_keywords"), list) else []
    context_kw = sel_cfg.get("pain_strict_context_keywords", []) if isinstance(sel_cfg.get("pain_strict_context_keywords"), list) else []

    pain_signal = _contains_any_keyword(hay, [str(x) for x in pain_kw])
    context_signal = _contains_any_keyword(hay, [str(x) for x in context_kw])
    periop_context = domain_flags.get("anesthesia_periop", False) or domain_flags.get("emergency_resus", False)
    icu_context = domain_flags.get("icu_ccm", False)

    design_context = clinical_intent and (context_signal or periop_context or icu_context)

    return pain_signal or design_context, {
        "pain_signal": pain_signal,
        "context_signal": context_signal,
        "design_context": design_context,
    }


def _journal_name(item: Dict[str, Any]) -> str:
    candidates = _journal_candidates(item)
    return candidates[0] if candidates else ""


def _score_item(
    item: Dict[str, Any], cfg: Dict[str, Any], *, haystack: str | None = None
) -> Tuple[float, List[str]]:
    sel = cfg.get("selection", {}) if isinstance(cfg.get("selection"), dict) else {}
    sc = cfg.get("scoring", {}) if isinstance(cfg.get("scoring"), dict) else {}
    kw = cfg.get("classification_keywords", {}) if isinstance(cfg.get("classification_keywords"), dict) else {}

    reasons: List[str] = []
    score = 0.0

    title = str(item.get("title") or "")
    hay = haystack if haystack is not None else _text_haystack(item)

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
    tiers = sel.get("tiers", {}) if isinstance(sel.get("tiers"), dict) else {}
    tier1 = tiers.get("tier1_core_clinical", []) if isinstance(tiers.get("tier1_core_clinical"), list) else []
    tier2 = tiers.get("tier2_general_high_impact", []) if isinstance(tiers.get("tier2_general_high_impact"), list) else []

    core = sel.get("core_journals", tier1)
    high = sel.get("high_impact_journals", tier2)
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
    min_score_overview = float(sel.get("min_score_overview", min_score))

    journal_mode = str(sel.get("journal_allowlist_mode", "prefer") or "prefer").strip().lower()
    core = sel.get("core_journals", [])
    core_set = {str(x).strip() for x in core} if isinstance(core, list) else set()
    tiers_cfg = sel.get("tiers", {}) if isinstance(sel.get("tiers"), dict) else {}
    domain_cfg = sel.get("domain_keywords", {}) if isinstance(sel.get("domain_keywords"), dict) else {}
    intent_cfg = sel.get("clinical_intent_keywords", {}) if isinstance(sel.get("clinical_intent_keywords"), dict) else {}
    hard_exclusion_patterns = sel.get("hard_exclusion_patterns", []) if isinstance(sel.get("hard_exclusion_patterns"), list) else []

    scored: List[
        Tuple[
            float,
            Dict[str, Any],
            List[str],
            str,
            bool,
            str,
            Dict[str, bool],
            Dict[str, bool],
            Dict[str, bool],
        ]
    ] = []
    excluded_by_allowlist = 0
    excluded_by_tier_rules = 0
    hard_excluded = 0

    for it in pubmed_items:
        hay = _text_haystack(it)

        if _matches_any_regex(hay, [str(x) for x in hard_exclusion_patterns]):
            hard_excluded += 1
            continue

        score, reasons = _score_item(it, cfg, haystack=hay)

        # Optional strict allowlist mode (not recommended by default)
        if journal_mode == "strict":
            if core_set and not _journal_matches(it, core_set):
                excluded_by_allowlist += 1
                continue

        tier = _journal_tier(it, tiers_cfg)
        domain_any, domain_flags = _domain_signals(hay, domain_cfg)
        clinical_intent, intent_flags = _clinical_intent(hay, intent_cfg)
        pain_ok, pain_flags = _pain_scope(hay, sel, domain_flags, clinical_intent)

        include_by_tier = False
        tier_reason = ""

        if tier == "tier1_core_clinical":
            include_by_tier = domain_any
            tier_reason = "tier1_domain_match" if include_by_tier else "tier1_domain_missing"
        elif tier == "tier2_general_high_impact":
            include_by_tier = domain_any and clinical_intent
            tier_reason = "tier2_domain+intent" if include_by_tier else "tier2_filtered"
        elif tier == "tier3_pain_strict":
            include_by_tier = pain_ok
            tier_reason = "tier3_pain_signal" if include_by_tier else "tier3_filtered"
        else:
            include_by_tier = domain_any or clinical_intent
            tier_reason = "untiered_domain" if include_by_tier else "untiered_filtered"

        scored.append(
            (
                score,
                it,
                reasons,
                tier,
                include_by_tier,
                tier_reason,
                domain_flags,
                intent_flags,
                pain_flags,
            )
        )

    scored.sort(key=lambda x: x[0], reverse=True)

    included: List[Dict[str, Any]] = []
    deep_dives: List[Dict[str, Any]] = []
    below_threshold = 0

    for score, it, reasons, tier, include_by_tier, tier_reason, domain_flags, intent_flags, pain_flags in scored:
        if not include_by_tier:
            excluded_by_tier_rules += 1
            continue

        threshold = min_score_overview if tier == "tier1_core_clinical" else min_score
        if score < threshold:
            below_threshold += 1
            continue

        if len(included) >= max_overview:
            break

        selection_reasons = list(reasons)
        if tier_reason:
            selection_reasons.append(tier_reason)
        if any(domain_flags.values()):
            selection_reasons.append("domain_signal")
        if any(intent_flags.values()):
            selection_reasons.append("clinical_intent")
        if tier == "tier3_pain_strict" and any(pain_flags.values()):
            selection_reasons.append("pain_scope")

        enriched = dict(it)
        rank = len(included) + 1
        enriched["cybermed_score"] = score
        enriched["cybermed_rank"] = rank
        enriched["cybermed_included"] = True
        enriched["cybermed_deep_dive"] = len(deep_dives) < max_deep_dives
        enriched["cybermed_selection_reasons"] = selection_reasons
        enriched["cybermed_tier"] = tier or "unclassified"
        enriched["cybermed_domain_flags"] = domain_flags
        enriched["cybermed_clinical_intent"] = intent_flags
        enriched["cybermed_pain_flags"] = pain_flags

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
        "excluded_by_tier_rules": excluded_by_tier_rules,
        "hard_excluded": hard_excluded,
        "below_threshold": below_threshold,
        "included": len(included),
        "deep_dives": len(deep_dives),
        "selected": len(included),
        "min_score": min_score,
        "min_score_overview": min_score_overview,
        "max_overview_items": max_overview,
        "max_deep_dives": max_deep_dives,
        "top_scores": [round(s[0], 2) for s in scored[: min(5, len(scored))]],
    }

    return SelectionResult(included=included, deep_dives=deep_dives, stats=stats)

