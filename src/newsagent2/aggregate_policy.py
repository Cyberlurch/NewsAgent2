"""Available-evidence reviews; missing history never becomes complete history.

The operator explicitly chose partial reviews rather than permanent calendar
blocks. This policy changes generation readiness, never SMTP retry authority.
"""
from __future__ import annotations

from datetime import date, timedelta

PARTIAL_REASONS = frozenset({
    "historical_policy_unknown", "edition_references_unknown", "edition_history_unknown",
    "required_daily_missing_or_unreferenced", "due_month_missing",
    "upstream_unknown", "upstream_incomplete",
})


def require_review_sources(coverage, *, available, category):
    """Permit known coverage gaps, reject every unknown/structural defect.

No exception is caught here. Corrupt inputs, bad references, duplicate periods,
foreign sources, open periods and empty unknown archives remain real failures.
"""
    reasons = set(coverage["reasons"])
    if coverage["status"] == "complete":
        return
    if not available:
        raise RuntimeError(category + ':' + coverage['status'] + ":no_available_sources")
    if not reasons or reasons - PARTIAL_REASONS:
        raise RuntimeError(category + ':' + coverage['status'] + ":invalid_source_chain")
    if reasons <= {"upstream_unknown", "upstream_incomplete"}:
        raise RuntimeError(category + ":unclassified_upstream_failure")


def require_closed(end, logical_date, cadence):
    if cadence in {"monthly", "yearly"} and date.fromisoformat(end) >= date.fromisoformat(logical_date):
        raise RuntimeError("edition_" + cadence + "_period_still_open")


def require_review_period(report_key, cadence, end, logical_date):
    require_closed(end, logical_date, cadence)
    if report_key == 'cybermed' and cadence == 'monthly':
        from .edition_plan import monthly_due
        next_month = date.fromisoformat(end)+timedelta(days=1)
        if date.fromisoformat(logical_date) < monthly_due(next_month.year, next_month.month):
            raise RuntimeError('edition_monthly_closing_slot_not_reached')


def require_year_closed(coverage):
    horizon = coverage['horizon']
    if horizon['open_months'] or horizon['future_months'] or horizon['awaiting_close']:
        raise RuntimeError('edition_yearly_closing_slot_not_reached')


def review_note(coverage, *, cadence, language="en"):
    """One concise reader note; detailed missing IDs stay in diagnostics."""
    if coverage["status"] == "complete":
        return ""
    expected = len(coverage['expected'])
    observed = len(set(coverage['observed']) & set(coverage['expected'])) if expected else len(coverage['observed'])
    counted = expected and not set(coverage['reasons']) & {'edition_references_unknown', 'historical_policy_unknown'}
    if language.lower().startswith('de'):
        noun = 'Monatsberichten' if cadence == 'yearly' else 'Tagesausgaben'
        detail = f'{observed} von {expected} gespeicherten {noun}' if counted else 'den vorhandenen gespeicherten Berichten'
        return f'> Teilrückblick auf Basis von {detail}; nicht verfügbare Ausgaben sind nicht enthalten.\n\n'
    if language.lower().startswith('sv'):
        noun = 'månadsrapporter' if cadence == 'yearly' else 'dagsutgåvor'
        detail = f'{observed} av {expected} sparade {noun}' if counted else 'tillgängliga sparade rapporter'
        return f'> Partiell översikt baserad på {detail}; saknade utgåvor ingår inte.\n\n'
    noun = 'monthly reports' if cadence == 'yearly' else 'daily editions'
    detail = f'{observed} of {expected} saved {noun}' if counted else 'the available saved reports'
    return f'> Partial review: based on {detail}; unavailable editions are omitted.\n\n'
