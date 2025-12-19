from __future__ import annotations

import json
import os
import re
from collections import Counter
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Dict, List, Tuple


@dataclass(frozen=True)
class SelectionResult:
    overview_items: List[Dict[str, Any]]
    deep_dive_items: List[Dict[str, Any]]
    stats: Dict[str, Any]
    deep_dive_candidates: List[Any] | None = None

    # Backward-compat convenience (legacy attribute name).
    @property
    def selected(self) -> List[Dict[str, Any]]:  # pragma: no cover - compatibility shim
        return self.overview_items

    # Backward-compat for callers still using .included/.deep_dives
    @property
    def included(self) -> List[Dict[str, Any]]:  # pragma: no cover - compatibility shim
        return self.overview_items

    @property
    def deep_dives(self) -> List[Dict[str, Any]]:  # pragma: no cover - compatibility shim
        return self.deep_dive_items


@dataclass(frozen=True)
class FoamedSelection:
    overview_items: List[Dict[str, Any]]
    top_picks: List[Dict[str, Any]]
    stats: Dict[str, Any]


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
    text = str(item.get("text") or item.get("summary") or "")
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
    *,
    requires_keywords: bool = True,
) -> Tuple[bool, Dict[str, bool]]:
    pain_kw = sel_cfg.get("pain_strict_keywords", []) if isinstance(sel_cfg.get("pain_strict_keywords"), list) else []
    context_kw = sel_cfg.get("pain_strict_context_keywords", []) if isinstance(sel_cfg.get("pain_strict_context_keywords"), list) else []

    pain_signal = _contains_any_keyword(hay, [str(x) for x in pain_kw])
    context_signal = _contains_any_keyword(hay, [str(x) for x in context_kw])
    periop_context = domain_flags.get("anesthesia_periop", False) or domain_flags.get("emergency_resus", False)
    icu_context = domain_flags.get("icu_ccm", False)

    design_context = clinical_intent and (context_signal or periop_context or icu_context)

    base_signal = pain_signal or design_context

    if not requires_keywords:
        base_signal = base_signal or domain_flags.get("anesthesia_periop", False) or domain_flags.get(
            "icu_ccm", False
        )

    return base_signal, {
        "pain_signal": pain_signal,
        "context_signal": context_signal,
        "design_context": design_context,
    }


def _journal_name(item: Dict[str, Any]) -> str:
    candidates = _journal_candidates(item)
    return candidates[0] if candidates else ""


def _extract_sample_size(hay: str) -> int:
    # Try structured patterns first (e.g., "n=1234")
    direct_match = re.search(r"\bn\s*[=:]\s*(\d{2,5})", hay, flags=re.IGNORECASE)
    if direct_match:
        return int(direct_match.group(1))

    # Heuristic: number followed by population noun
    noun_match = re.search(
        r"(\d{2,5})\s+(patients|participants|subjects|cases|adults|children|neonates|infants)",
        hay,
        flags=re.IGNORECASE,
    )
    if noun_match:
        return int(noun_match.group(1))

    return 0


def _deep_dive_score(
    item: Dict[str, Any],
    hay: str,
    cfg: Dict[str, Any],
    *,
    domain_flags: Dict[str, bool],
    high_journals: set[str] | None = None,
    additional_penalty: float = 0.0,
) -> Tuple[float, List[str]]:
    dd_cfg = cfg.get("deep_dive_scoring", {}) if isinstance(cfg.get("deep_dive_scoring"), dict) else {}
    weights = dd_cfg.get("weights", {}) if isinstance(dd_cfg.get("weights"), dict) else {}

    w_study = float(weights.get("study_design", 3.0))
    w_pubtype = float(weights.get("publication_type", 2.5))
    w_power = float(weights.get("power", 1.5))
    w_sample = float(weights.get("sample_size", 1.2))
    w_predictive = float(weights.get("predictive", 1.5))
    w_clinical = float(weights.get("clinical_relevance", 1.0))
    w_downrank = float(weights.get("downrank", -1.5))

    def _kw_list(key: str) -> List[str]:
        val = dd_cfg.get(key, [])
        return [str(x) for x in val] if isinstance(val, list) else []

    study_design_kw = _kw_list("study_design_signals")
    publication_types_kw = [s.lower() for s in _kw_list("publication_type_signals")]
    power_kw = _kw_list("power_signals")
    predictive_kw = _kw_list("predictive_value_signals")
    clinical_kw = _kw_list("clinical_relevance_keywords")
    downrank_kw = _kw_list("downrank_signals")
    preclinical_kw = _kw_list("preclinical_penalty_signals")
    editorial_kw = _kw_list("editorial_penalty_signals")

    reasons: List[str] = []
    score = 0.0

    if _contains_any_keyword(hay, study_design_kw):
        score += w_study
        reasons.append("design_signal")

    pub_types = item.get("publication_types")
    if isinstance(pub_types, list):
        normalized_pub_types = {str(x).strip().lower() for x in pub_types}
        for pt in publication_types_kw:
            if pt.lower() in normalized_pub_types:
                score += w_pubtype
                reasons.append(f"pubtype:{pt}")
                break

    n = _extract_sample_size(hay)
    power_hit = _contains_any_keyword(hay, power_kw)
    if n > 0:
        size_score = min(3.0, n / 500.0) * w_sample
        score += size_score
        reasons.append(f"n={n}")
    elif power_hit:
        score += w_power * 0.5
        reasons.append("power_hint")

    if power_hit:
        score += w_power
        reasons.append("power_kw")

    if _contains_any_keyword(hay, predictive_kw):
        score += w_predictive
        reasons.append("predictive_signal")

    if _contains_any_keyword(hay, clinical_kw):
        score += w_clinical
        reasons.append("clinical_relevance")

    if _contains_any_keyword(hay, downrank_kw):
        score += w_downrank
        reasons.append("downrank_signal")

    if _contains_any_keyword(hay, preclinical_kw):
        score += w_downrank * 1.2
        reasons.append("preclinical_penalty")

    if _contains_any_keyword(hay, editorial_kw):
        score += w_downrank
        reasons.append("editorial_penalty")

    if high_journals and _journal_matches(item, high_journals):
        if _contains_any_keyword(hay, preclinical_kw):
            score += w_downrank * 0.8
            reasons.append("high_impact_preclinical_penalty")

    for domain_name, flag in domain_flags.items():
        if flag:
            reasons.append(f"domain:{domain_name}")

    if additional_penalty:
        score += additional_penalty
        reasons.append(f"manual_penalty({additional_penalty})")

    return score, reasons


def _domain_key_for_quota(flags: Dict[str, bool]) -> str:
    active = [k for k, v in flags.items() if v]
    return active[0] if active else "general"


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
            overview_items=list(pubmed_items),
            deep_dive_items=list(pubmed_items),
            stats={
                "enabled": False,
                "candidates": len(pubmed_items),
                "included": len(pubmed_items),
                "included_overview": len(pubmed_items),
                "deep_dives": len(pubmed_items),
                "selected_deep_dives": len(pubmed_items),
                "config_path": path,
                "max_selected_per_run": len(pubmed_items),
                "max_selected": len(pubmed_items),
            },
        )

    sel = cfg.get("selection", {}) if isinstance(cfg.get("selection"), dict) else {}

    max_overview = int(
        sel.get(
            "overview_max_per_run",
            sel.get("max_overview_items", sel.get("max_selected_per_run", 12)),
        )
        or 12
    )
    max_deep_dives = int(sel.get("deep_dive_max_per_run", sel.get("max_deep_dives", 8)) or 8)
    min_score = float(sel.get("min_score_to_select", 2.0))
    min_score_overview = float(sel.get("min_score_overview", min_score))
    min_score_deep_dive = float(sel.get("min_score_deep_dive", min_score))
    pain_requires_keywords = bool(sel.get("pain_requires_keywords", True))
    reconsider_unsent_hours = int(sel.get("reconsider_unsent_hours", 36) or 36)

    journal_mode = str(sel.get("journal_allowlist_mode", "prefer") or "prefer").strip().lower()
    core = sel.get("core_journals", [])
    core_set = {str(x).strip() for x in core} if isinstance(core, list) else set()
    high = sel.get("high_impact_journals", [])
    high_set = {str(x).strip() for x in high} if isinstance(high, list) else set()
    tiers_cfg = sel.get("tiers", {}) if isinstance(sel.get("tiers"), dict) else {}
    # Support both legacy and new tier labels for compatibility.
    tier1_list = tiers_cfg.get("tier1_core") or tiers_cfg.get("tier1_core_clinical") or []
    tier2_list = tiers_cfg.get("tier2_high_impact") or tiers_cfg.get("tier2_general_high_impact") or []
    tier3_list = tiers_cfg.get("tier3_pain_strict") or []
    domain_cfg = sel.get("domain_keywords", {}) if isinstance(sel.get("domain_keywords"), dict) else {}
    intent_cfg = sel.get("clinical_intent_keywords", {}) if isinstance(sel.get("clinical_intent_keywords"), dict) else {}
    hard_exclusion_patterns = sel.get("hard_exclusion_patterns", []) if isinstance(sel.get("hard_exclusion_patterns"), list) else []
    hard_exclude_overview_raw = sel.get("hard_exclude_overview_regex", hard_exclusion_patterns)
    hard_exclude_overview = [str(x) for x in hard_exclude_overview_raw] if isinstance(hard_exclude_overview_raw, list) else []
    hard_exclude_deep_raw = sel.get("hard_exclude_deep_dive_regex", [])
    hard_exclude_deep = [str(x) for x in hard_exclude_deep_raw] if isinstance(hard_exclude_deep_raw, list) else []
    deprioritize_title_patterns = sel.get("exclude_title_regex", []) if isinstance(sel.get("exclude_title_regex"), list) else []
    pubtype_exclusions = (
        sel.get("publication_type_exclusions", [])
        if isinstance(sel.get("publication_type_exclusions"), list)
        else []
    )

    overview_pool: List[Dict[str, Any]] = []
    excluded_by_allowlist = 0
    excluded_overview_offtopic = 0
    below_threshold_overview = 0
    deep_dive_hard_excluded = 0
    excluded_deep_dive_low_score = 0
    included_core = 0
    included_high_impact = 0
    screened_candidates = len(pubmed_items)

    def _tier_priority(tier: str) -> int:
        if tier.startswith("tier1"):
            return 0
        if tier.startswith("tier2"):
            return 1
        if tier.startswith("tier3"):
            return 2
        return 3

    for it in pubmed_items:
        hay = _text_haystack(it)

        if _matches_any_regex(hay, [str(x) for x in hard_exclude_overview]):
            excluded_overview_offtopic += 1
            continue

        title = str(it.get("title") or "")
        pub_types_raw = it.get("publication_types")
        pubtype_penalty_hit = False
        if isinstance(pub_types_raw, list):
            normalized_pub_types = {str(x).strip().lower() for x in pub_types_raw}
            pubtype_penalty_hit = any(pt.lower() in normalized_pub_types for pt in pubtype_exclusions)

        title_penalty_hit = _matches_any_regex(title, [str(x) for x in deprioritize_title_patterns])

        tier = _journal_tier(it, {"tier1_core": tier1_list, "tier2_high_impact": tier2_list, "tier3_pain_strict": tier3_list})
        if not tier and _journal_matches(it, core_set):
            tier = "tier1_core_fallback"
        elif not tier and _journal_matches(it, high_set):
            tier = "tier2_high_impact_fallback"
        domain_any, domain_flags = _domain_signals(hay, domain_cfg)
        clinical_intent, intent_flags = _clinical_intent(hay, intent_cfg)
        pain_ok, pain_flags = _pain_scope(
            hay,
            sel,
            domain_flags,
            clinical_intent,
            requires_keywords=pain_requires_keywords,
        )

        score, reasons = _score_item(it, cfg, haystack=hay)
        sc = cfg.get("scoring", {}) if isinstance(cfg.get("scoring"), dict) else {}
        if pubtype_penalty_hit:
            penalty = float(sc.get("publication_type_penalty", -1.5))
            score += penalty
            reasons.append(f"pubtype_penalty({penalty})")
        deep_score, deep_reasons = _deep_dive_score(
            it,
            hay,
            cfg,
            domain_flags=domain_flags,
            high_journals=high_set,
            additional_penalty=(
                float(sc.get("deep_dive_pubtype_penalty", -1.5)) if pubtype_penalty_hit else 0.0
            )
            + (float(sc.get("deep_dive_title_penalty", -1.0)) if title_penalty_hit else 0.0),
        )

        include_by_tier = False
        tier_reason = ""

        if journal_mode == "strict" and core_set and not _journal_matches(it, core_set):
            excluded_by_allowlist += 1
            excluded_overview_offtopic += 1
            continue

        if tier in ("tier1_core", "tier1_core_clinical"):
            include_by_tier = True
            tier_reason = "tier1_core_default"
        elif tier in ("tier2_general_high_impact", "tier2_high_impact", "tier2_high_impact_fallback"):
            high_domain = (
                domain_flags.get("anesthesia_periop", False)
                or domain_flags.get("icu_ccm", False)
                or domain_flags.get("emergency_resus", False)
            )
            include_by_tier = high_domain or (domain_any and clinical_intent)
            tier_reason = "tier2_domain_or_intent" if include_by_tier else "tier2_filtered"
        elif tier == "tier3_pain_strict":
            include_by_tier = pain_ok
            tier_reason = "tier3_pain_signal" if include_by_tier else "tier3_filtered"
        else:
            include_by_tier = domain_any or clinical_intent or _journal_matches(it, core_set) or _journal_matches(it, high_set)
            tier_reason = "untiered_domain" if include_by_tier else "untiered_filtered"

        if not include_by_tier:
            excluded_overview_offtopic += 1
            continue

        threshold = None if tier in ("tier1_core", "tier1_core_clinical") else min_score_overview
        if threshold is not None and score < threshold:
            below_threshold_overview += 1
            continue

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
        enriched["cybermed_score"] = score
        enriched["cybermed_rank"] = len(overview_pool) + 1
        enriched["cybermed_included"] = True
        enriched["cybermed_deep_dive"] = False
        enriched["cybermed_deep_dive_score"] = deep_score
        enriched["cybermed_deep_dive_reasons"] = selection_reasons + deep_reasons
        enriched["cybermed_selection_reasons"] = selection_reasons
        enriched["cybermed_tier"] = tier or "unclassified"
        enriched["cybermed_domain_flags"] = domain_flags
        enriched["cybermed_clinical_intent"] = intent_flags
        enriched["cybermed_pain_flags"] = pain_flags

        enriched["cybermed_deep_dive_hard_excluded"] = _matches_any_regex(
            hay, [str(x) for x in hard_exclude_deep]
        )
        if enriched["cybermed_deep_dive_hard_excluded"]:
            deep_dive_hard_excluded += 1
            enriched["cybermed_deep_dive_reasons"].append("hard_exclude_deep_dive")

        if tier in ("tier1_core", "tier1_core_clinical"):
            included_core += 1
        if tier in ("tier2_general_high_impact", "tier2_high_impact"):
            included_high_impact += 1

        overview_pool.append(enriched)

    # Deep dive diversification and ranking
    overview_sorted = sorted(
        overview_pool,
        key=lambda x: (
            _tier_priority(str(x.get("cybermed_tier") or "")),
            -float(x.get("cybermed_score", 0.0)),
            -float(x.get("cybermed_deep_dive_score", 0.0)),
        ),
    )

    overview_items: List[Dict[str, Any]] = []
    for idx, cand in enumerate(overview_sorted, start=1):
        if len(overview_items) >= max_overview:
            break
        cand["cybermed_rank"] = idx
        overview_items.append(cand)

    deep_dive_items: List[Dict[str, Any]] = []
    reason_counter: Counter[str] = Counter()
    dd_cfg = cfg.get("deep_dive_scoring", {}) if isinstance(cfg.get("deep_dive_scoring"), dict) else {}
    max_per_domain_dd = int(sel.get("max_per_domain_deep_dive", dd_cfg.get("max_per_domain", 3)) or 3)

    deep_candidates = sorted(
        overview_items,
        key=lambda x: (
            float(x.get("cybermed_deep_dive_score", 0.0)),
            float(x.get("cybermed_score", 0.0)),
        ),
        reverse=True,
    )

    domain_counts: Dict[str, int] = {}
    for cand in deep_candidates:
        if len(deep_dive_items) >= max_deep_dives:
            break

        if cand.get("cybermed_deep_dive_hard_excluded"):
            continue

        if float(cand.get("cybermed_deep_dive_score", 0.0)) < min_score_deep_dive:
            excluded_deep_dive_low_score += 1
            continue

        domain_key = _domain_key_for_quota(cand.get("cybermed_domain_flags", {}))
        if domain_counts.get(domain_key, 0) >= max_per_domain_dd:
            continue

        domain_counts[domain_key] = domain_counts.get(domain_key, 0) + 1
        cand["cybermed_deep_dive"] = True
        cand["cybermed_deep_dive_rank"] = len(deep_dive_items) + 1
        deep_dive_items.append(cand)
        for r in cand.get("cybermed_deep_dive_reasons", []):
            reason_counter[str(r)] += 1

    deep_dive_candidates = [
        str(
            cand.get("id")
            or cand.get("uid")
            or cand.get("pmid")
            or cand.get("doi")
            or cand.get("url")
            or cand.get("title")
            or idx
        )
        for idx, cand in enumerate(deep_candidates, start=1)
    ][:max_deep_dives]

    # Non-sensitive stats only (no titles, no URLs, no recipients).
    stats = {
        "enabled": True,
        "config_path": path,
        "journal_allowlist_mode": journal_mode,
        "candidates": len(pubmed_items),
        "excluded_by_allowlist": excluded_by_allowlist,
        "excluded_overview_offtopic": excluded_overview_offtopic,
        "below_threshold_overview": below_threshold_overview,
        "included": len(overview_items),
        "included_overview": len(overview_items),
        "deep_dives": len(deep_dive_items),
        "selected_deep_dives": len(deep_dive_items),
        "included_core": included_core,
        "included_high_impact": included_high_impact,
        "selected": len(overview_items),
        "below_threshold_deep_dive": excluded_deep_dive_low_score,
        "excluded_deep_dive_low_score": excluded_deep_dive_low_score,
        "deep_dive_hard_excluded": deep_dive_hard_excluded,
        "screened_candidates": screened_candidates,
        "min_score": min_score,
        "min_score_overview": min_score_overview,
        "min_score_deep_dive": min_score_deep_dive,
        "reconsider_unsent_hours": reconsider_unsent_hours,
        "max_overview_items": max_overview,
        "max_deep_dives": max_deep_dives,
        "max_selected_per_run": max_overview,
        "max_selected": max_overview,
        "top_scores": [round(float(it.get("cybermed_score", 0.0)), 2) for it in overview_sorted[: min(5, len(overview_sorted))]],
        "deep_dive_reason_counts": dict(reason_counter.most_common(8)),
    }

    return SelectionResult(
        overview_items=overview_items,
        deep_dive_items=deep_dive_items,
        stats=stats,
        deep_dive_candidates=deep_dive_candidates,
    )


def _foamed_domain_score(hay: str) -> Tuple[float, Dict[str, bool], bool]:
    """
    Lightweight clinical relevance signals for FOAMed/blog posts.

    Returns (score, flags, pain_blocked_for_context).
    """

    flags = {
        "anesthesia_periop": _contains_any_keyword(
            hay,
            [
                "anesthesia",
                "anaesthesia",
                "anesthesiology",
                "perioperative",
                "operating room",
                "or theater",
                "neuraxial",
                "epidural",
                "spinal",
                "block",
                "regional anesthesia",
            ],
        ),
        "icu_ccm": _contains_any_keyword(
            hay,
            [
                "icu",
                "intensive care",
                "critical care",
                "ventilator",
                "mechanical ventilation",
                "ards",
                "ecmo",
                "hemodynamic",
                "haemodynamic",
                "vasopressor",
                "norepinephrine",
                "sedation",
                "delirium",
                "crrt",
            ],
        ),
        "emergency_resus": _contains_any_keyword(
            hay,
            [
                "resuscitation",
                "cardiac arrest",
                "prehospital",
                "ems",
                "emergency department",
                "ed",
            ],
        ),
        "airway_resp": _contains_any_keyword(
            hay,
            [
                "airway",
                "intubation",
                "extubation",
                "ventilation",
                "respiratory",
                "oxygenation",
            ],
        ),
        "infection_sepsis": _contains_any_keyword(
            hay,
            [
                "sepsis",
                "infection",
                "antibiotic",
                "antimicrobial",
                "pneumonia",
            ],
        ),
        "hemodynamics": _contains_any_keyword(
            hay,
            [
                "shock",
                "blood pressure",
                "circulation",
                "hemodynamic",
                "haemodynamic",
                "vasopressor",
                "inotrope",
            ],
        ),
    }

    pain_hit = _contains_any_keyword(
        hay,
        [
            "pain",
            "analgesia",
            "opioid",
            "opioids",
            "nerve block",
            "regional block",
            "fascial plane",
        ],
    )
    pain_context = flags["anesthesia_periop"] or flags["icu_ccm"] or flags["emergency_resus"] or flags["airway_resp"] or flags["hemodynamics"]
    pain_context = pain_context or _contains_any_keyword(
        hay,
        ["ultrasound-guided", "catheter", "perioperative", "postoperative", "perineural"],
    )

    pain_blocked = False
    if pain_hit and pain_context:
        flags["pain_regional"] = True
    elif pain_hit:
        flags["pain_regional"] = False
        pain_blocked = True
    else:
        flags["pain_regional"] = False

    score = sum(1 for v in flags.values() if v)
    return float(score), flags, pain_blocked


def _load_curated_foamed_sources(path: str = "data/cybermed_foamed_sources.json") -> set[str]:
    try:
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
        if not isinstance(data, list):
            return set()
        names = [str(entry.get("name") or "") for entry in data if isinstance(entry, dict)]
        return {n.strip().lower() for n in names if n.strip()}
    except Exception:
        return set()


def select_cybermed_foamed_items(
    foamed_items: List[Dict[str, Any]],
    *,
    max_overview: int = 40,
    max_top_picks: int = 2,
) -> FoamedSelection:
    """
    Broad-but-relevant selector for FOAMed/blog content.

    - Includes items with any domain signal (anesthesia/ICU/resus/airway/infection/hemodynamics).
    - Excludes clearly off-domain or promotional posts.
    - Applies stricter pain gating (pain topics must have perioperative/regional/ICU context).
    - Marks up to `max_top_picks` items as top picks by score.
    """

    now = datetime.now(timezone.utc)
    screened_candidates = len(foamed_items)
    excluded_offdomain = 0
    foamed_excluded_offtopic = 0
    foamed_excluded_no_signal = 0
    excluded_pain_context = 0

    scored: List[Dict[str, Any]] = []
    fallback_candidates: List[Dict[str, Any]] = []
    off_domain_patterns = [
        r"\b(job|jobs|career|vacancy)\b",
        r"sponsor",
        r"advertisement",
        r"promo",
        r"\b(ad|ads)\b",
        r"\bsponsor",
        r"site update",
        r"tickets?",
        r"conference",
        r"course",
        r"webinar registration",
        r"merch",
        r"store",
        r"shop",
        r"newsletter",
    ]
    weak_medical_cues = [
        "icu",
        "intensive care",
        "ventilation",
        "ventilator",
        "airway",
        "sepsis",
        "shock",
        "anesthesia",
        "anaesthesia",
        "analgesia",
        "block",
        "resuscitation",
        "ecmo",
        "sedation",
        "vasopressor",
        "vasopressors",
        "perioperative",
        "trauma",
    ]
    curated_sources = _load_curated_foamed_sources()
    if not curated_sources:
        curated_sources = {
            str(it.get("foamed_source") or it.get("channel") or it.get("source") or "").strip().lower()
            for it in foamed_items
            if (it.get("foamed_source") or it.get("channel") or it.get("source"))
        }

    for it in foamed_items:
        hay = _text_haystack(it)
        title = str(it.get("title") or "")
        excerpt = str(it.get("text") or "")
        if not excerpt.strip():
            hay = (title or "").lower()
        source_name = str(it.get("foamed_source") or it.get("channel") or it.get("source") or "").strip().lower()
        is_curated_source = source_name in curated_sources

        if _matches_any_regex(hay, off_domain_patterns):
            excluded_offdomain += 1
            foamed_excluded_offtopic += 1
            continue

        score, flags, pain_blocked = _foamed_domain_score(hay)
        weak_signal = _contains_any_keyword(hay, weak_medical_cues)
        has_signal = score > 0 or weak_signal
        default_include = is_curated_source and weak_signal

        if pain_blocked and not default_include:
            excluded_pain_context += 1
            foamed_excluded_no_signal += 1
            continue

        if not has_signal:
            foamed_excluded_no_signal += 1
            fallback_candidates.append(dict(it))
            continue

        published = it.get("published_at")
        if isinstance(published, datetime) and published.tzinfo is None:
            published = published.replace(tzinfo=timezone.utc)

        recency_bonus = 0.0
        age_hours = None
        if isinstance(published, datetime):
            age_hours = (now - published).total_seconds() / 3600.0
            if age_hours < 6:
                recency_bonus = 0.6
            elif age_hours < 12:
                recency_bonus = 0.4
            elif age_hours < 24:
                recency_bonus = 0.2

        base_score = score if score > 0 else 0.3 if default_include else 0.0
        total_score = base_score + recency_bonus
        enriched = dict(it)
        enriched["foamed_score"] = round(total_score, 3)
        enriched["foamed_flags"] = flags
        enriched["foamed_age_hours"] = age_hours
        enriched["foamed_default_include"] = default_include
        scored.append(enriched)

    overview_sorted = sorted(
        scored,
        key=lambda x: (
            -float(x.get("foamed_score", 0.0)),
            x.get("published_at") or datetime.min.replace(tzinfo=timezone.utc),
        ),
    )

    overview_items: List[Dict[str, Any]] = []
    for cand in overview_sorted:
        if len(overview_items) >= max_overview:
            break
        overview_items.append(cand)

    fallback_used = False
    if not overview_items and fallback_candidates:
        fallback_used = True
        fallback_sorted = sorted(
            fallback_candidates,
            key=lambda x: x.get("published_at") or datetime.min.replace(tzinfo=timezone.utc),
            reverse=True,
        )
        overview_items.extend(fallback_sorted[:max_overview])

    top_picks: List[Dict[str, Any]] = []
    for cand in overview_items[:max_top_picks]:
        cand["top_pick"] = True
        top_picks.append(cand)

    stats = {
        "screened_candidates": screened_candidates,
        "excluded_offdomain": excluded_offdomain,
        "excluded_pain_context": excluded_pain_context,
        "included_overview": len(overview_items),
        "top_picks": len(top_picks),
        "max_overview_items": max_overview,
        "max_top_picks": max_top_picks,
        "foamed_included": len(overview_items),
        "foamed_excluded_offtopic": foamed_excluded_offtopic,
        "foamed_excluded_no_signal": foamed_excluded_no_signal,
        "fallback_used": fallback_used,
        "fallback_candidates": len(fallback_candidates),
    }

    return FoamedSelection(overview_items=overview_items, top_picks=top_picks, stats=stats)
