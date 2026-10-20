"""Narrow grammatical rewrites of narrator frames, independent of channel lists.

No inference about truth or reliability. Unknown syntax stays unchanged; dates,
numbers, negation, modal verbs and qualifications remain source text.
"""
from __future__ import annotations

import re


EN_VERBS = r'says|said|states|stated|reports|reported|argues|contends|explains|explained|notes|claims|asserts|specifies|adds|suggests|warns|predicts|describes'
_NAMED = re.compile(r'^(?P<name>[A-ZÄÖÜÅÆØ][\w’\'.-]+(?:\s+[A-ZÄÖÜÅÆØ][\w’\'.-]+){1,5})\s+(?P<verb>'+EN_VERBS+r')\b')
_HONORIFIC = re.compile(r'^(?:Professor|Prof\.|Dr\.|Actress|Actor|Pastor)\s+')


def narrator_name(value):
    match = _NAMED.match(str(value or '').strip())
    return _HONORIFIC.sub('', match['name']) if match else ''


def block_speakers(item):
    """A repeated reporting subject must also introduce this item's synopsis.

    A politician merely quoted in a news bullet is not assumed to be the host.
    The returned name is shown once in the source heading, including guests.
    """
    source_basis = item.get('source_basis')
    if (item.get('content_status') == 'metadata_only' or item.get('text_source') == 'metadata_only'
            or isinstance(source_basis,dict) and source_basis.get('coverage') == 'none'):
        return ()
    name = narrator_name(item.get('transcript_full_summary') or item.get('summary'))
    if not name: return ()
    fields = [text for key in ('transcript_key_points', 'important_details', 'transcript_notable_claims')
              for text in (item[key] if isinstance(item.get(key), list) else [])]
    own_name = name.casefold() == str(item.get('channel') or '').strip().casefold()
    return (name,) if own_name or sum(narrator_name(text) == name for text in fields) >= 2 else ()


def named_frame(value, speakers):
    for speaker in speakers:
        match = re.match(r'^(?:(?:Professor|Prof\.|Dr\.|Actress|Actor|Pastor)\s+)?'+re.escape(speaker)
            +r'\s+(?P<verb>'+EN_VERBS+r')\s+(?P<link>that\s+)?(?P<rest>.+)$', value, re.I)
        if match: return match
    return None


def english_relation(value, speakers):
    prefixes = [r'The (?:speaker|presenter|narrator)', *[re.escape(name) for name in speakers]]
    match = re.match(r'^(?:'+'|'.join(prefixes)+r') (?:ties|links) (.+? and .+?) to '
        r'((?:the|a|an|these|those|two|three|four|five|six|\d+)\b.+)$', value, re.I)
    # An explicitly coordinated subject has unambiguous plural agreement.
    return match[1]+' are linked to '+match[2] if match else value


_DE_FRAME = re.compile(r'^(?:Der (?:Beitrag|Bericht|Sprecher)|Das (?:Video|Transkript))\s+'
    r'(?:behauptet|berichtet|erklärt|betont|sagt|führt aus|stellt fest|nennt),?\s+dass\s+(.+)$', re.I)
_NOUN = r'[A-ZÄÖÜ][\wÄÖÜäöüß.-]*'
_ADJECTIVE = r'[a-zäöüß][\wäöüß-]*'
_NP = r'(?:(?:der|die|das|ein|eine|einer|eines|einem|einen|diese|dieser|dieses|kein|keine)\s+(?:'+_ADJECTIVE+r'\s+)*'+_NOUN+r'|(?:knapp\s+)?\d+\s+'+_NOUN+r'|'+_NOUN+r'(?:\s+'+_NOUN+r')*|es|sie|er|wir|ihr|man)'
_SUBJECT = re.compile(r'^('+_NP+r')\s+(.+)$')
_OCCURRENCE = re.compile(r'^((?:in|auf|von|für|an|aus|mit|ohne|über)\s+'+_NP+r')\s+(stattfindet|stattfinden)$')
_FINITE = set(('ist sind war waren sei seien wäre wären hat haben hatte hatten hätte hätten habe '
    'wird werden wurde wurden werde würde würden kann können könnte könnten soll sollen sollte sollten '
    'muss müssen müsse müssten darf dürfen dürfte dürften kippte kippten verhinderte verhinderten '
    'beschloss betrifft betreffen zeigt zeigen gilt gelten gab gibt führt führen').split())
_SEPARABLE = {'stattfindet':('findet','statt'), 'stattfinden':('finden','statt'),
              'bereitstellten':('stellten','bereit'), 'bereitstellte':('stellte','bereit')}


def _front_verb(predicate):
    words = predicate.split()
    if not words: return None
    if words[0] in _FINITE: return predicate
    last = words[-1]
    if last not in _FINITE and last not in _SEPARABLE: return None
    head, tail = _SEPARABLE.get(last, (last,''))
    return ' '.join([head, *words[:-1], *([tail] if tail else [])])


def _de_clause(clause):
    match = _SUBJECT.match(clause.strip())
    if not match: return None
    subject, predicate = match.groups()
    occurrence = _OCCURRENCE.match(predicate)
    if occurrence:
        return subject+' '+occurrence[1]+' '+_front_verb(occurrence[2])
    # Genitive/dative and subject-modifier/verb-argument attachments cannot be
    # distinguished by this small recognizer. Never guess their ownership.
    if re.match(r'^(?:der|des|in|auf|von|für|an|aus|mit|ohne|über|bei|vor|nach|zu)\b',predicate):
        return None
    coordinated = re.split(r'\s+(und|oder|aber)\s+', predicate)
    # Shared-subject actions with a clearly adverbial second clause. Nominal
    # coordinations ('orchestriert und kein Zufall ist') remain one predicate.
    if (len(coordinated) == 3 and re.match(r'^(?:so|dadurch|damit|anschließend|auch)\b', coordinated[2])
            and all(_front_verb(p) for p in coordinated[::2])):
        return subject+' '+_front_verb(coordinated[0])+' '+coordinated[1]+' '+_front_verb(coordinated[2])
    # Moving only the final verb of two separate actions would change grammar.
    if len(coordinated)>1 and any(p.split()[-1] in _FINITE for p in coordinated[:-1:2]): return None
    direct = _front_verb(predicate)
    return subject+' '+direct if direct else None


def german_statement(value):
    match = _DE_FRAME.match(value)
    if not match:
        tools = re.match(r'^Der (?:Beitrag|Bericht) nennt (.+? und .+?) als '
            r'((?:Mechanismus|Mittel|Instrument|Weg)\b.*)$', value)
        if tools: return tools[1]+' dienen als '+tools[2]
        witness = re.match(r'^Der (?:Beitrag|Bericht) zitiert Interviews, wonach ('+_NP+r') '
            r'(?:behaupten|sagen|berichten), (.+)$', value)
        return witness[1]+' berichten in Interviews, '+witness[2] if witness else value
    text = match[1]
    if any(mark in text for mark in ('"','„','“',';')): return value
    # Keep subordinate explanations intact and only move a recognized finite
    # verb in the first clause. Decimal commas do not split clauses.
    clauses = re.split(r',\s+(?!\d)', text, maxsplit=1)
    first = clauses[0].rstrip('.!?')
    direct = _de_clause(first)
    if not direct: return value
    if len(clauses)>1: return direct+', '+clauses[1]
    return direct+(text[-1] if text[-1] in '.!?' else '')
