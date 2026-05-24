from __future__ import annotations

import json
import os
import re
from collections import Counter
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Dict, List, Tuple, Set


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




LOW_PRIORITY_PUB_TYPES = {"editorial", "letter", "comment", "news", "erratum"}
HIGH_EVIDENCE_PUB_TYPES = {
    "randomized controlled trial",
    "clinical trial",
    "systematic review",
    "meta-analysis",
    "practice guideline",
    "guideline",
}

REAL_EVIDENCE_TYPES = {
    "randomized controlled trial",
    "clinical trial",
    "systematic review",
    "meta-analysis",
    "practice guideline",
    "guideline",
    "observational clinical study",
}


def _pub_types(item: Dict[str, Any]) -> Set[str]:
    return {str(x).strip().lower() for x in (item.get("publication_types") or []) if str(x).strip()}


def _evidence_tags(item: Dict[str, Any]) -> Set[str]:
    return {str(x).strip().lower() for x in (item.get("evidence_tags") or []) if str(x).strip()}


def _low_priority_only(pub_types: Set[str]) -> bool:
    return bool(pub_types) and pub_types.issubset(LOW_PRIORITY_PUB_TYPES)


def _has_real_evidence_pubtype(pub_types: Set[str]) -> bool:
    return bool(pub_types & REAL_EVIDENCE_TYPES)

def _content_length_bucket(length: int) -> str:
    if length <= 0:
        return "none"
    if length < 160:
        return "short"
    if length < 600:
        return "medium"
    return "long"

def _pubmed_v1_scores(item: Dict[str, Any], hay: str, domain_flags: Dict[str, bool]) -> Tuple[Dict[str, float], List[str], List[str], str | None, bool]:
    reasons: List[str] = []
    penalties: List[str] = []
    hard_exclusion: str | None = None
    pub_types = _pub_types(item)
    evidence_tags = _evidence_tags(item)
    journal = str(item.get("journal") or "").lower()
    text = str(item.get("text") or "")
    has_content = bool(text.strip())
    content_length = int(item.get("content_length") or len(text.strip()))
    has_structured = bool(item.get("abstract_sections"))
    source = str(item.get("content_source") or "")

    evidence = 0.0
    clinical = 0.0
    practice = 0.0
    text_conf = 0.0

    if pub_types & HIGH_EVIDENCE_PUB_TYPES or (evidence_tags & HIGH_EVIDENCE_PUB_TYPES):
        if _low_priority_only(pub_types) and not _has_real_evidence_pubtype(pub_types):
            penalties.append("evidence_tag_overridden_by_publication_type")
        else:
            evidence += 3.0; reasons.append("high_evidence_publication_type")
    if _contains_any_keyword(hay,["prospective cohort","diagnostic accuracy","randomized","clinical trial"]):
        evidence += 1.5
    if pub_types & LOW_PRIORITY_PUB_TYPES:
        evidence -= 1.5; penalties.append("publication_type_low_priority")
    if _contains_any_keyword(hay,["animal","murine","in vitro","preclinical","basic science"]):
        evidence -= 1.5; penalties.append("possible_offtopic")

    clinical_terms=["mortality","intubation","airway","ventilation","ards","sepsis","septic shock","vasopressor","resuscitation","cardiac arrest","trauma","anesthesia safety","perioperative","regional anesthesia","analgesia","icu length of stay","delirium","sedation","antimicrobial","infection","hemodynamic"]
    clinical_hits=sum(1 for t in clinical_terms if t in hay)
    clinical += min(4.0, clinical_hits*0.7)
    if any(domain_flags.values()): clinical += 1.0; reasons.append("strong_clinical_relevance")
    if clinical < 1.0: penalties.append("low_clinical_relevance")

    if _contains_any_keyword(hay,["guideline","consensus","recommendation"]): practice += 2.0; reasons.append("guideline_or_consensus")
    if _contains_any_keyword(hay,["randomized","rct"]): practice += 1.5; reasons.append("randomized_trial")
    if _contains_any_keyword(hay,["systematic review","meta-analysis"]): practice += 1.5; reasons.append("systematic_review_or_meta_analysis")
    if _contains_any_keyword(hay,["mortality","length of stay","safety","adverse event"]): practice += 1.0; reasons.append("patient_centered_outcomes")
    if _contains_any_keyword(journal,["nejm","jama","lancet","bmj"]): practice += 0.8; reasons.append("high_impact_journal")

    if has_content: text_conf += 1.0
    if source in {"pubmed_abstract","pmc_oa_fulltext","unpaywall_fulltext"}: text_conf += 0.7
    if has_structured: text_conf += 0.5
    if content_length >= 200: text_conf += 0.6
    elif content_length < 80: text_conf -= 0.8; penalties.append("insufficient_text")
    if not has_content: text_conf -= 1.2; penalties.append("insufficient_text")

    if not any(domain_flags.values()) and clinical_hits==0:
        penalties.append("weak_domain_signal")
    if evidence <= 0.5:
        penalties.append("low_evidence_strength")

    if _contains_any_keyword(hay,["marketing","stock market","sports betting","cryptocurrency"]):
        hard_exclusion="clearly_non_clinical"

    total = evidence + clinical + practice + text_conf
    return {"evidence":evidence,"clinical":clinical,"practice":practice,"text":text_conf,"total":total}, reasons, penalties, hard_exclusion, has_content

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
    publication_type_penalty_hits = 0
    title_penalty_hits = 0
    tier_counts: Counter[str] = Counter()
    domain_signal_counts: Counter[str] = Counter()
    clinical_intent_counts: Counter[str] = Counter()
    soft_penalty_counts: Counter[str] = Counter()
    exclusion_reason_counts: Counter[str] = Counter()
    top_pick_reason_counts: Counter[str] = Counter()
    top_pick_floor_rejection_counts: Counter[str] = Counter()
    deep_dive_reason_counts_v1: Counter[str] = Counter()
    deep_dive_floor_rejection_counts: Counter[str] = Counter()
    low_evidence_radar_reason_counts: Counter[str] = Counter()
    raw_selection_audit_reason_counts: Counter[str] = Counter()
    relevance_dist: Counter[str] = Counter()
    evidence_dist: Counter[str] = Counter()
    practice_dist: Counter[str] = Counter()
    hard_excluded_total = 0
    kept_after_soft_screen = 0
    low_priority_publication_type_excluded_total = 0
    overview_eligible_after_type_floor_total = 0
    overview_excluded_by_type_floor_total = 0
    low_evidence_radar_candidates_total = 0
    raw_selection_audit_total = len(pubmed_items)
    raw_selection_audit_overview_eligible_total = 0
    raw_selection_audit_low_evidence_radar_total = 0
    raw_selection_audit_top_pick_candidate_total = 0
    raw_selection_audit_deep_dive_candidate_total = 0

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
            hard_excluded_total += 1
            exclusion_reason_counts["configured_hard_exclusion"] += 1
            continue

        title = str(it.get("title") or "")
        pub_types_raw = it.get("publication_types")
        pubtype_penalty_hit = False
        if isinstance(pub_types_raw, list):
            normalized_pub_types = {str(x).strip().lower() for x in pub_types_raw}
            pubtype_penalty_hit = any(pt.lower() in normalized_pub_types for pt in pubtype_exclusions)
        if pubtype_penalty_hit:
            publication_type_penalty_hits += 1

        title_penalty_hit = _matches_any_regex(title, [str(x) for x in deprioritize_title_patterns])
        if title_penalty_hit:
            title_penalty_hits += 1

        tier = _journal_tier(it, {"tier1_core": tier1_list, "tier2_high_impact": tier2_list, "tier3_pain_strict": tier3_list})
        if not tier and _journal_matches(it, core_set):
            tier = "tier1_core_fallback"
        elif not tier and _journal_matches(it, high_set):
            tier = "tier2_high_impact_fallback"
        tier_counts[tier or "unclassified"] += 1
        domain_any, domain_flags = _domain_signals(hay, domain_cfg)
        clinical_intent, intent_flags = _clinical_intent(hay, intent_cfg)
        for flag, active in domain_flags.items():
            if active:
                domain_signal_counts[str(flag)] += 1
        for flag, active in intent_flags.items():
            if active:
                clinical_intent_counts[str(flag)] += 1
        pain_ok, pain_flags = _pain_scope(
            hay,
            sel,
            domain_flags,
            clinical_intent,
            requires_keywords=pain_requires_keywords,
        )

        v1_scores, v1_reasons, v1_penalties, v1_hard_exclusion, has_usable_content = _pubmed_v1_scores(it, hay, domain_flags)
        pub_types = _pub_types(it)
        has_real_evidence_pubtype = _has_real_evidence_pubtype(pub_types)
        low_priority_only = _low_priority_only(pub_types)
        is_correction_or_commentary, floor_block_reasons = _is_correction_or_commentary_item(title, pub_types)
        title_floor_reasons = _title_correspondence_or_commentary_reasons(title)
        title_floor_blocked = bool(title_floor_reasons) and not has_real_evidence_pubtype
        strong_domain_and_usable = any(domain_flags.values()) and has_usable_content
        floor_reasons: List[str] = []
        if float(v1_scores.get("evidence", 0.0)) > 0:
            floor_reasons.append("evidence_strength_positive")
        if float(v1_scores.get("clinical", 0.0)) > 0:
            floor_reasons.append("clinical_relevance_positive")
        if float(v1_scores.get("practice", 0.0)) > 0:
            floor_reasons.append("practice_changing_positive")
        if has_real_evidence_pubtype:
            floor_reasons.append("real_evidence_publication_type")
        if strong_domain_and_usable:
            floor_reasons.append("strong_domain_plus_usable_content")
        if float(v1_scores.get("practice",0.0)) > 0 and float(v1_scores.get("clinical",0.0)) > 0 and has_usable_content:
            floor_reasons.append("practice_and_clinical_with_content")
        if float(v1_scores.get("clinical",0.0)) >= 3 and has_usable_content and ("journal article" in {p.lower() for p in pub_types}) and not low_priority_only:
            floor_reasons.append("very_strong_clinical_journal_article")
        type_floor_passed = bool(floor_reasons)
        low_evidence_radar = (
            low_priority_only
            and float(v1_scores.get("evidence", 0.0)) <= 0
            and float(v1_scores.get("clinical", 0.0)) <= 0
            and float(v1_scores.get("practice", 0.0)) <= 0
        )
        if low_evidence_radar:
            low_evidence_radar_candidates_total += 1
            low_evidence_radar_reason_counts["low_evidence_news_or_commentary"] += 1
        if low_priority_only and not has_real_evidence_pubtype and ("clinical_trial" in _evidence_tags(it)):
            low_evidence_radar_reason_counts["evidence_tag_overridden_by_publication_type"] += 1
        if type_floor_passed:
            raw_selection_audit_overview_eligible_total += 1
        if low_evidence_radar:
            raw_selection_audit_low_evidence_radar_total += 1
        if (
            type_floor_passed and has_usable_content and float(v1_scores.get("evidence", 0.0)) > 0
            and (float(v1_scores.get("clinical", 0.0)) > 0 or float(v1_scores.get("practice", 0.0)) > 0)
            and not low_priority_only
        ):
            raw_selection_audit_top_pick_candidate_total += 1
            raw_selection_audit_deep_dive_candidate_total += 1
        raw_selection_audit_reason_counts["type_floor_passed" if type_floor_passed else "type_floor_failed"] += 1
        if v1_hard_exclusion:
            excluded_overview_offtopic += 1
            hard_excluded_total += 1
            exclusion_reason_counts[v1_hard_exclusion] += 1
            continue
        allow_despite_commentary = has_real_evidence_pubtype or (
            float(v1_scores.get("practice", 0.0)) > 0 and float(v1_scores.get("clinical", 0.0)) > 0 and has_usable_content
        )
        if (is_correction_or_commentary and not allow_despite_commentary) or low_evidence_radar:
            low_evidence_radar_candidates_total += 1
            for rr in (floor_block_reasons or ["low_evidence_commentary"]): exclusion_reason_counts[rr] += 1
            if float(v1_scores.get("evidence",0.0)) <= 0 and "review" in title.lower(): exclusion_reason_counts["low_evidence_narrative_review"] += 1
            if not floor_reasons: exclusion_reason_counts["no_primary_evidence_signal"] += 1
            overview_excluded_by_type_floor_total += 1
            low_priority_publication_type_excluded_total += 1
            continue
        if title_floor_blocked:
            low_evidence_radar_candidates_total += 1
            overview_excluded_by_type_floor_total += 1
            low_priority_publication_type_excluded_total += 1
            for rr in title_floor_reasons:
                low_evidence_radar_reason_counts[rr] += 1
                exclusion_reason_counts[rr] += 1
            continue
        if not type_floor_passed:
            overview_excluded_by_type_floor_total += 1
            exclusion_reason_counts["no_primary_evidence_signal"] += 1
            continue
        overview_eligible_after_type_floor_total += 1

        score, reasons = _score_item(it, cfg, haystack=hay)
        score += float(v1_scores.get("total",0.0))
        reasons.extend(v1_reasons)
        for pen in v1_penalties:
            soft_penalty_counts[pen] += 1
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
            score -= 1.0
            soft_penalty_counts["possible_offtopic"] += 1
            reasons.append("possible_offtopic")

        threshold = None if tier in ("tier1_core", "tier1_core_clinical") else min_score_overview
        if threshold is not None and score < threshold:
            below_threshold_overview += 1
            exclusion_reason_counts["below_threshold_overview"] += 1
            continue
        kept_after_soft_screen += 1

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
        enriched["evidence_strength_score"] = round(float(v1_scores.get("evidence",0.0)),3)
        enriched["clinical_relevance_score"] = round(float(v1_scores.get("clinical",0.0)),3)
        enriched["practice_changing_score"] = round(float(v1_scores.get("practice",0.0)),3)
        enriched["content_length_bucket"] = _content_length_bucket(int(it.get("content_length") or len(str(it.get("text") or "").strip())))
        enriched["has_usable_content"] = bool(has_usable_content)
        enriched["reason_labels"] = list(dict.fromkeys((selection_reasons+v1_reasons+v1_penalties)))[:12]
        if low_priority_only and not has_real_evidence_pubtype and ("clinical_trial" in _evidence_tags(it)):
            enriched["reason_labels"] = list(dict.fromkeys(enriched["reason_labels"] + ["evidence_tag_overridden_by_publication_type"]))
        enriched["type_floor_passed"] = type_floor_passed
        enriched["overview_eligible"] = type_floor_passed and not low_evidence_radar
        enriched["low_evidence_radar"] = low_evidence_radar
        enriched["floor_rejection_reason"] = "" if enriched["overview_eligible"] else (floor_block_reasons[0] if floor_block_reasons else ("low_evidence_commentary" if low_evidence_radar else "no_primary_evidence_signal"))
        _attach_evidence_hint_labels(enriched, foamed=False)
        label = str(enriched.get("evidence_strength_label") or "").upper()
        d_label = label == "D"
        e_label = label == "E"
        if d_label:
            if not (
                int(enriched.get("clinical_relevance_1_5") or 0) >= 4
                and int(enriched.get("practice_change_potential_1_5") or 0) >= 3
                and has_usable_content
                and not title_floor_blocked
                and not is_correction_or_commentary
            ):
                enriched["low_evidence_radar"] = True
                enriched["overview_eligible"] = False
                enriched["floor_rejection_reason"] = "evidence_d_context_radar"
                low_evidence_radar_reason_counts["evidence_d_context_radar"] += 1
        if e_label and not has_real_evidence_pubtype:
            enriched["low_evidence_radar"] = True
            enriched["overview_eligible"] = False
            enriched["floor_rejection_reason"] = "evidence_e_not_normal_paper"
            if "evidence_e_not_normal_paper" not in enriched.get("reason_labels", []):
                enriched["reason_labels"] = list(enriched.get("reason_labels", [])) + ["evidence_e_not_normal_paper"]
            low_evidence_radar_reason_counts["evidence_e_not_normal_paper"] += 1

        enriched["cybermed_deep_dive_hard_excluded"] = _matches_any_regex(
            hay, [str(x) for x in hard_exclude_deep]
        )
        if enriched["cybermed_deep_dive_hard_excluded"]:
            deep_dive_hard_excluded += 1
            enriched["cybermed_deep_dive_reasons"].append("hard_exclude_deep_dive")

        relevance_dist[str(int(score))] += 1
        evidence_dist[str(int(float(enriched.get("evidence_strength_score",0.0))))] += 1
        practice_dist[str(int(float(enriched.get("practice_changing_score",0.0))))] += 1

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
    final_context_radar_items: List[Dict[str, Any]] = []
    final_excluded_by_floor_reason_counts: Counter[str] = Counter()
    for idx, cand in enumerate(overview_sorted, start=1):
        cand["cybermed_rank"] = idx
        floor_reasons = _final_floor_reasons(cand)
        cand["final_context_floor_reasons"] = floor_reasons
        if floor_reasons:
            cand["overview_eligible"] = False
            cand["low_evidence_radar"] = True
            if "evidence_d_context_radar" in floor_reasons:
                cand["floor_rejection_reason"] = "evidence_d_context_radar"
            elif not str(cand.get("floor_rejection_reason") or "").strip():
                cand["floor_rejection_reason"] = floor_reasons[0]
            for r in floor_reasons:
                final_excluded_by_floor_reason_counts[r] += 1
                low_evidence_radar_reason_counts[r] += 1
            final_context_radar_items.append(cand)
            continue
        if len(overview_items) >= max_overview:
            continue
        overview_items.append(cand)

    for cand in overview_items:
        top_pick = (
            float(cand.get("cybermed_score",0.0)) >= max(min_score_overview+2.5,6.0)
            and float(cand.get("evidence_strength_score",0.0)) >= 2.0
            and float(cand.get("clinical_relevance_score",0.0)) >= 2.0
            and str(cand.get("content_length_bucket")) != "none"
        )
        ev_label = str(cand.get("evidence_strength_label") or "").upper()
        has_real_evidence_pubtype = _has_real_evidence_pubtype(_pub_types(cand))
        title_floor_blocked = bool(_title_correspondence_or_commentary_reasons(str(cand.get("title") or ""))) and not has_real_evidence_pubtype
        if top_pick and (ev_label not in {"A", "B", "C"} or cand.get("low_evidence_radar") or not cand.get("type_floor_passed") or _is_correction_or_commentary_item(str(cand.get("title") or ""), _pub_types(cand))[0] or title_floor_blocked or str(cand.get("floor_rejection_reason") or "").strip()):
            top_pick = False
            top_pick_floor_rejection_counts["type_floor"] += 1
        if top_pick and float(cand.get("evidence_strength_score",0.0)) <= 0:
            top_pick = False
            top_pick_floor_rejection_counts["evidence_strength_score"] += 1
        if top_pick and not (float(cand.get("clinical_relevance_score",0.0)) > 0 or float(cand.get("practice_changing_score",0.0)) > 0):
            top_pick = False
            top_pick_floor_rejection_counts["clinical_or_practice"] += 1
        cand["top_pick"] = bool(top_pick)
        star_reasons=[r for r in cand.get("reason_labels",[]) if r in {"guideline_or_consensus","randomized_trial","systematic_review_or_meta_analysis","high_impact_journal","patient_centered_outcomes","strong_clinical_relevance"}]
        if top_pick and not star_reasons:
            star_reasons=["practice_changing_signal"]
        cand["star_reasons"] = star_reasons
        for sr in star_reasons: top_pick_reason_counts[sr]+=1

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
        ev_label = str(cand.get("evidence_strength_label") or "").upper()
        has_real_evidence_pubtype = _has_real_evidence_pubtype(_pub_types(cand))
        title_floor_blocked = bool(_title_correspondence_or_commentary_reasons(str(cand.get("title") or ""))) and not has_real_evidence_pubtype
        if cand.get("low_evidence_radar") or ev_label not in {"A", "B", "C"} or _is_correction_or_commentary_item(str(cand.get("title") or ""), _pub_types(cand))[0] or title_floor_blocked or str(cand.get("floor_rejection_reason") or "").strip() or not bool(cand.get("has_usable_content")):
            deep_dive_floor_rejection_counts["low_evidence_news_or_commentary"] += 1
            continue

        if float(cand.get("cybermed_deep_dive_score", 0.0)) < min_score_deep_dive:
            excluded_deep_dive_low_score += 1
            continue

        domain_key = _domain_key_for_quota(cand.get("cybermed_domain_flags", {}))
        if domain_counts.get(domain_key, 0) >= max_per_domain_dd:
            continue

        domain_counts[domain_key] = domain_counts.get(domain_key, 0) + 1
        if float(cand.get("evidence_strength_score",0.0)) <= 0:
            excluded_deep_dive_low_score += 1
            deep_dive_floor_rejection_counts["evidence_strength_score"] += 1
            continue
        if float(cand.get("practice_changing_score",0.0)) <= 0 and float(cand.get("clinical_relevance_score",0.0)) <= 0:
            excluded_deep_dive_low_score += 1
            deep_dive_floor_rejection_counts["clinical_or_practice"] += 1
            continue
        cand["cybermed_deep_dive"] = True
        cand["cybermed_deep_dive_rank"] = len(deep_dive_items) + 1
        deep_dive_items.append(cand)
        compact=[]
        if "randomized_trial" in cand.get("reason_labels",[]): compact.append("RCT")
        if "guideline_or_consensus" in cand.get("reason_labels",[]): compact.append("guideline")
        if "systematic_review_or_meta_analysis" in cand.get("reason_labels",[]): compact.append("systematic_review")
        if "high_impact_journal" in cand.get("reason_labels",[]): compact.append("high_impact_journal")
        if "patient_centered_outcomes" in cand.get("reason_labels",[]): compact.append("patient_centered_outcome")
        if cand.get("cybermed_domain_flags",{}).get("icu_ccm"): compact.append("strong_ICU_relevance")
        if cand.get("cybermed_domain_flags",{}).get("emergency_resus"): compact.append("strong_EM_relevance")
        if cand.get("cybermed_domain_flags",{}).get("anesthesia_periop"): compact.append("strong_anesthesia_relevance")
        cand["deep_dive_reasons"] = compact
        for r in compact: deep_dive_reason_counts_v1[r]+=1
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
        "selection_diagnostics": {
            "excluded_overview_offtopic": excluded_overview_offtopic,
            "below_threshold_overview": below_threshold_overview,
            "excluded_by_allowlist": excluded_by_allowlist,
            "publication_type_penalty_hits": publication_type_penalty_hits,
            "title_penalty_hits": title_penalty_hits,
            "tier_counts": dict(tier_counts),
            "domain_signal_counts": dict(domain_signal_counts),
            "clinical_intent_counts": dict(clinical_intent_counts),
            "pubmed_relevance_score_distribution": dict(relevance_dist),
            "pubmed_evidence_strength_score_distribution": dict(evidence_dist),
            "pubmed_practice_changing_score_distribution": dict(practice_dist),
            "pubmed_domain_signal_counts": dict(domain_signal_counts),
            "pubmed_exclusion_reason_counts": dict(exclusion_reason_counts),
            "pubmed_soft_penalty_reason_counts": dict(soft_penalty_counts),
            "pubmed_top_pick_reason_counts": dict(top_pick_reason_counts),
            "pubmed_top_pick_floor_rejection_counts": dict(top_pick_floor_rejection_counts),
            "pubmed_deep_dive_reason_counts": dict(deep_dive_reason_counts_v1),
            "pubmed_deep_dive_floor_rejection_counts": dict(deep_dive_floor_rejection_counts),
            "pubmed_low_evidence_radar_candidates_total": low_evidence_radar_candidates_total,
            "pubmed_low_evidence_radar_reason_counts": dict(low_evidence_radar_reason_counts),
            "pubmed_low_priority_publication_type_excluded_total": low_priority_publication_type_excluded_total,
            "pubmed_context_radar_candidates_total": low_evidence_radar_candidates_total,
            "pubmed_context_radar_reason_counts": dict(low_evidence_radar_reason_counts),
            "pubmed_correspondence_reply_excluded_total": int(low_evidence_radar_reason_counts.get("correspondence_or_reply", 0) + low_evidence_radar_reason_counts.get("commentary_title_pattern", 0)),
            "pubmed_evidence_e_excluded_from_papers_total": int(low_evidence_radar_reason_counts.get("evidence_e_not_normal_paper", 0)),
            "pubmed_evidence_d_context_radar_total": int(low_evidence_radar_reason_counts.get("evidence_d_context_radar", 0)),
            "pubmed_context_radar_publication_type_counts": dict(Counter(str(pt) for it in pubmed_items for pt in (it.get("publication_types") or []))),
            "pubmed_overview_eligible_after_type_floor_total": overview_eligible_after_type_floor_total,
            "pubmed_overview_excluded_by_type_floor_total": overview_excluded_by_type_floor_total,
            "pubmed_raw_selection_audit_total": raw_selection_audit_total,
            "pubmed_raw_selection_audit_overview_eligible_total": raw_selection_audit_overview_eligible_total,
            "pubmed_raw_selection_audit_low_evidence_radar_total": raw_selection_audit_low_evidence_radar_total,
            "pubmed_raw_selection_audit_top_pick_candidate_total": raw_selection_audit_top_pick_candidate_total,
            "pubmed_raw_selection_audit_deep_dive_candidate_total": raw_selection_audit_deep_dive_candidate_total,
            "pubmed_raw_selection_audit_reason_counts": dict(raw_selection_audit_reason_counts),
            "pubmed_candidates_kept_after_soft_screen_total": kept_after_soft_screen,
            "pubmed_candidates_hard_excluded_total": hard_excluded_total,
            "pubmed_final_selected_overview_total": len(overview_items),
            "pubmed_final_selected_evidence_label_counts": dict(Counter(str(it.get("evidence_strength_label") or "") for it in overview_items)),
            "pubmed_final_selected_evidence_label_basis_counts": dict(Counter(str(it.get("evidence_strength_label_basis") or "") for it in overview_items)),
            "pubmed_final_selected_context_floor_violations_total": sum(1 for it in overview_items if it.get("final_context_floor_reasons")),
            "pubmed_final_selected_context_floor_violation_counts": dict(Counter(r for it in overview_items for r in (it.get("final_context_floor_reasons") or []))),
            "pubmed_final_excluded_by_evidence_floor_total": len(final_context_radar_items),
            "pubmed_final_excluded_by_evidence_floor_reason_counts": dict(final_excluded_by_floor_reason_counts),
            "pubmed_final_context_radar_candidates_total": len(final_context_radar_items),
            "pubmed_final_top_pick_floor_rejection_counts": dict(top_pick_floor_rejection_counts),
            "pubmed_final_deep_dive_floor_rejection_counts": dict(deep_dive_floor_rejection_counts),
            "pubmed_final_selected_preview": [
                {
                    "evidence_strength_label": str(it.get("evidence_strength_label") or ""),
                    "evidence_strength_label_basis": str(it.get("evidence_strength_label_basis") or ""),
                    "publication_types": [str(v) for v in (it.get("publication_types") or [])][:5],
                    "evidence_tags": [str(v) for v in (it.get("evidence_tags") or [])][:5],
                    "content_source": str(it.get("content_source") or ""),
                    "content_length_bucket": str(it.get("content_length_bucket") or "none"),
                    "clinical_relevance_1_5": int(it.get("clinical_relevance_1_5") or 0),
                    "practice_change_potential_1_5": int(it.get("practice_change_potential_1_5") or 0),
                    "text_confidence_label": str(it.get("text_confidence_label") or ""),
                    "top_pick": bool(it.get("top_pick")),
                    "deep_dive_candidate": bool(it.get("cybermed_deep_dive")),
                }
                for it in overview_items[:10]
            ],
            "top_candidate_score_preview": [
                {
                    "score": round(float(it.get("cybermed_score", 0.0)), 3),
                    "evidence_strength_score": round(float(it.get("evidence_strength_score", 0.0)), 3),
                    "clinical_relevance_score": round(float(it.get("clinical_relevance_score", 0.0)), 3),
                    "practice_changing_score": round(float(it.get("practice_changing_score", 0.0)), 3),
                    "deep_dive_score": round(float(it.get("cybermed_deep_dive_score", 0.0)), 3),
                    "tier": str(it.get("cybermed_tier") or "unclassified"),
                    "publication_types": [str(v) for v in (it.get("publication_types") or [])][:5],
                    "evidence_tags": [str(v) for v in (it.get("evidence_tags") or [])][:5],
                    "content_source": str(it.get("content_source") or ""),
                    "content_length_bucket": str(it.get("content_length_bucket") or "none"),
                    "type_floor_passed": bool(it.get("type_floor_passed")),
                    "overview_eligible": bool(it.get("overview_eligible")),
                    "low_evidence_radar": bool(it.get("low_evidence_radar")),
                    "evidence_strength_label": str(it.get("evidence_strength_label") or ""),
                    "evidence_strength_label_basis": str(it.get("evidence_strength_label_basis") or ""),
                    "floor_rejection_reason": str(it.get("floor_rejection_reason") or ""),
                    "reason_labels": [str(v) for v in (it.get("reason_labels") or [])][:10],
                    "exclusion_reason": str(it.get("exclusion_reason") or ""),
                }
                for it in overview_sorted[:10]
            ],
        },
    }

    return SelectionResult(
        overview_items=overview_items,
        deep_dive_items=deep_dive_items,
        stats=stats,
        deep_dive_candidates=deep_dive_candidates,
    )


def _final_floor_reasons(item: Dict[str, Any]) -> List[str]:
    reasons: List[str] = []
    ev_label = str(item.get("evidence_strength_label") or "").upper()
    ev_basis = str(item.get("evidence_strength_label_basis") or "")
    ev_score = float(item.get("evidence_strength_score", 0.0) or 0.0)
    reason_labels = {str(r) for r in (item.get("reason_labels") or [])}
    blocked_labels = {
        "evidence_e_not_normal_paper",
        "correspondence_or_reply",
        "commentary_title_pattern",
        "correction_or_erratum",
        "comment_on_article",
        "low_evidence_news_or_commentary",
        "low_evidence_editorial_letter",
        "no_primary_evidence_signal",
    }
    if ev_label == "E":
        reasons.append("evidence_label_e")
    if ev_basis in {"low_evidence_publication_type", "metadata_only"}:
        reasons.append(f"evidence_basis_{ev_basis}")
    if ev_basis == "score_fallback" and ev_score <= 0:
        reasons.append("evidence_score_fallback_non_positive")
    if str(item.get("floor_rejection_reason") or "").strip():
        reasons.append("floor_rejection_reason")
    if bool(item.get("low_evidence_radar")):
        reasons.append("low_evidence_radar")
    for lbl in sorted(blocked_labels & reason_labels):
        reasons.append(lbl)

    if ev_label == "D":
        d_ok = (
            bool(item.get("has_usable_content"))
            and int(item.get("clinical_relevance_1_5") or 0) >= 4
            and int(item.get("practice_change_potential_1_5") or 0) >= 3
            and ev_basis not in {"low_evidence_publication_type", "metadata_only"}
            and not (blocked_labels & reason_labels)
        )
        if not d_ok:
            reasons.append("evidence_d_context_radar")
    return list(dict.fromkeys(reasons))


def _is_correction_or_commentary_item(title: str, pub_types: List[str]) -> Tuple[bool, List[str]]:
    tl = (title or '').lower()
    pts = {str(x).strip().lower() for x in (pub_types or [])}
    reasons=[]
    if any(x in pts for x in {'published erratum','erratum','correction'} ) or 'correction to:' in tl:
        reasons.append('correction_or_erratum')
    if 'comment on' in tl:
        reasons.append('comment_on_article')
    if any(x in pts for x in {'letter','comment','editorial','news'}):
        reasons.append('low_evidence_editorial_letter')
    return (len(reasons)>0,reasons)


def _title_correspondence_or_commentary_reasons(title: str) -> List[str]:
    tl = (title or "").lower()
    correspondence_patterns = [
        "reply to ",
        "response to ",
        "in reply",
        "author reply",
        "reply:",
    ]
    commentary_patterns = [
        "comment on",
        "commentary",
        "correspondence",
        "letter to the editor",
        "correction to:",
        "erratum",
        "corrigendum",
        "editorial",
    ]
    reasons: List[str] = []
    if any(p in tl for p in correspondence_patterns):
        reasons.append("correspondence_or_reply")
    if any(p in tl for p in commentary_patterns):
        reasons.append("commentary_title_pattern")
    return reasons


def _attach_evidence_hint_labels(item: Dict[str, Any], *, foamed: bool=False) -> None:
    """Attach display-ready heuristic labels (not official GRADE).

    True GRADE is a body-of-evidence framework; these are article-level evidence strength hints only.
    """
    if foamed:
        item['source_quality_label']=str(item.get('source_quality_label') or ('core' if 'core' in str(item.get('foamed_source','')).lower() else 'important'))
        item['text_confidence_label']='high' if str(item.get('content_length_bucket')) in {'long','very_long'} else ('moderate' if str(item.get('content_length_bucket')) in {'medium','short'} else 'low')
        item['clinical_usefulness_1_5']=max(1,min(5,int(round(float(item.get('clinical_relevance_score',0.0))+1))))
        item['practice_relevance_1_5']=max(1,min(5,int(round(float(item.get('practice_changing_score',0.0))+1))))
        return
    ev=float(item.get('evidence_strength_score',0.0) or 0.0)
    pub_types = {str(v).strip().lower() for v in (item.get("publication_types") or []) if str(v).strip()}
    ev_tags = {str(v).strip().lower() for v in (item.get("evidence_tags") or []) if str(v).strip()}
    hay = f"{item.get('title') or ''} {item.get('abstract') or ''} {item.get('text') or ''}".lower()
    has_patient_outcome = any(k in hay for k in ["mortality", "intubation", "ventilation", "shock", "sepsis", "cardiac arrest", "safety", "patient-centered", "patient centred"])
    has_guideline = any(x in pub_types for x in {"practice guideline", "guideline"}) or ("consensus" in hay and "recommend" in hay)
    is_meta_or_sr = any(x in pub_types for x in {"meta-analysis", "systematic review"}) or any(x in hay for x in ["meta-analysis", "meta analysis", "systematic review"])
    is_rct = ("randomized controlled trial" in pub_types) or ("randomized" in hay and "trial" in hay) or ("phase 3" in hay or "phase iii" in hay)
    is_observational = any(x in hay for x in ["prospective cohort", "retrospective cohort", "registry", "observational", "secondary analysis", "interrupted time series", "before-after"])
    is_low_evidence_type = any(x in pub_types for x in {"editorial", "letter", "comment", "news", "published erratum", "erratum", "correction"})
    is_metadata_only = not str(item.get("text") or "").strip() and not str(item.get("abstract") or "").strip()
    is_narrative_or_expert = any(x in hay for x in ["narrative review", "how i do it", "expert opinion", "expert synthesis", "physiology"])
    basis = "score_fallback"
    if is_metadata_only:
        lbl, basis = "E", "metadata_only"
    elif is_low_evidence_type:
        lbl, basis = "E", "low_evidence_publication_type"
    elif has_guideline:
        lbl, basis = ("A" if has_patient_outcome else "B"), "guideline_or_consensus"
    elif is_meta_or_sr:
        lbl, basis = ("A" if (has_patient_outcome or is_rct) else "B"), "meta_analysis_clinical"
    elif is_rct:
        lbl, basis = ("A" if has_patient_outcome else "B"), "randomized_trial_clinical"
    elif is_observational:
        lbl, basis = ("B" if ("prospective cohort" in hay or "registry" in hay) and has_patient_outcome else "C"), "observational_clinical"
    elif is_narrative_or_expert:
        lbl, basis = "D", "expert_review"
    elif ev>=4: lbl='A'
    elif ev>=3: lbl='B'
    elif ev>=2: lbl='C'
    elif ev>0: lbl='D'
    else: lbl='E'
    item['evidence_strength_label']=lbl
    item['evidence_strength_label_basis']=basis
    item['evidence_strength_score_0_5']=max(0,min(5,int(round(ev))))
    rel_base = float(item.get('clinical_relevance_score',0.0) or 0.0)
    impact_base = float(item.get('practice_changing_score',0.0) or 0.0)
    direct_clinical_bonus = 0.4 if any(k in hay for k in ["icu", "critical care", "emergency", "anesthesia", "anaesthesia", "perioperative", "bedside"]) else 0.0
    outcome_bonus = 0.5 if has_patient_outcome else 0.0
    rel = int(round(rel_base + 1 + direct_clinical_bonus + outcome_bonus))
    impact_bonus = 0.5 if (has_guideline or is_meta_or_sr or is_rct) and has_patient_outcome else (0.2 if is_observational and has_patient_outcome else 0.0)
    if is_narrative_or_expert:
        impact_bonus -= 0.4
    impact = int(round(impact_base + 1 + impact_bonus))
    item['clinical_relevance_1_5']=max(1,min(5,rel))
    item['practice_change_potential_1_5']=max(1,min(5,impact))
    item['text_confidence_label']='high' if str(item.get('content_length_bucket')) in {'long','very_long'} else ('moderate' if str(item.get('content_length_bucket')) in {'medium','short'} else 'low')
    item['evidence_label_reason']=f"article-level heuristic: {basis}"
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

    for _it in overview_items:
        _attach_evidence_hint_labels(_it, foamed=True)
    return FoamedSelection(overview_items=overview_items, top_picks=top_picks, stats=stats)
