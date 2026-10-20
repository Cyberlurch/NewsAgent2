"""Source perspectives and concrete consequences, without a fact-checking voice."""
import copy
import json
from types import SimpleNamespace

import pytest

from newsagent2 import reporter, summarizer
from newsagent2.cyberlurch_readability import reader_prose, SOURCE_VOICE_RULE
from newsagent2.cyberlurch_monthly import monthly_prompt
from newsagent2.cyberlurch_yearly import yearly_prompt


def row(**extra):
    return {"id":"port", "title":"Harbor access proposal", "channel":"Harbor Desk", "url":"https://example.test/port",
        "published_at":"2026-10-15", "content_status":"full_text", "text_source":"managed_transcript",
        "transcript_full_summary":"The speaker reports that the outer harbor could close for six weeks, not six months.",
        "transcript_key_points":["The speaker states that freight operators oppose the plan.",
            "The speaker asserts that the council has not approved the plan.", "The speaker notes that fuel imports would move to another terminal."],
        "important_details":["Truck journeys could increase by 40 kilometres because the alternate terminal is farther inland.",
            "Freight firms expect higher delivery costs if the proposal takes effect."], **extra}


@pytest.mark.parametrize("verb", ["says", "reports", "states", "asserts", "claims", "argues", "explains", "notes", "emphasizes", "teaches", "specifies", "suggests", "adds"])
def test_reporting_wrapper_can_go_while_source_negation_modal_numbers_remain(verb):
    assert reader_prose(f"The speaker {verb} that the harbor could close for six weeks, not six months.") == "The harbor could close for six weeks, not six months."


@pytest.mark.parametrize("text", [
    "Jane Smith reports that the council has not approved the plan.",
    "The speaker denies that the council has approved the plan.",
    "The speaker does not claim that the plan will work.",
    "The speaker predicts that the plan will fail.",
    "The speaker reports that the port is closed and argues that the mayor should resign.",
    "The speaker suggests that a delay occurred.",
    "The speaker specifies that the port is closed and adds that the mayor should resign.",
    "The speaker says that the port is closed but claims that another entrance is open.",
    "The speaker states that the port could close and emphasizes that approval is still missing.",
    "The speaker specifies that the city has 16 beds and that the region serves 1.5 million people.",
    "Der Beitrag bestreitet, dass das Abkommen die Kosten senken könnte.",
    "Talaren säger att hamnen kan stängas nästa vecka.",
])
def test_meaning_bearing_attribution_or_unreorderable_legacy_clause_is_not_blindly_deleted(text):
    assert reader_prose(text) == text


def test_only_the_already_identified_channel_wrapper_is_removed():
    assert reader_prose("According to Harbor Desk, The port could close.", channel="Harbor Desk") == "The port could close."
    assert reader_prose("According to Jane Smith, the port could close.", channel="Harbor Desk") == "According to Jane Smith, the port could close."


@pytest.mark.parametrize("deep", [False, True])
def test_snapshot_brief_but_body_preserves_actual_effects_beyond_three_headline_points(deep):
    item=row(cyberlurch_daily_deep_dive=deep)
    before=copy.deepcopy(item)
    md=reporter.render_cyberlurch_daily_report([item],title="Cyberlurch",generated_at="now")
    snapshot,body=md.split("## Selected items",1)
    assert "The outer harbor could close for six weeks, not six months." in snapshot
    assert "40 kilometres" not in snapshot and "40 kilometres" in body and "higher delivery costs" in body
    assert "has not approved" in body and "could" in body
    assert "The speaker" not in md and "According to" not in md and "Channel identity:" not in md
    assert "*Harbor Desk ·" in body and item==before


def test_distinct_channel_accents_and_conflicting_numbers_are_not_averaged_or_filtered():
    positive=row(id="optimist",channel="Trade Outlook",transcript_full_summary="The speaker argues that the proposal could lower freight costs.",
        transcript_key_points=["The route could cost 10 million euros each year.","The route could cost 12 million euros each year."],important_details=[])
    negative=row(id="skeptic",channel="Port Watch",transcript_full_summary="The speaker argues that the proposal could increase freight costs.",
        transcript_key_points=["The council did not approve the plan."],important_details=[])
    md=reporter.render_cyberlurch_daily_report([positive,negative],title="Cyberlurch",generated_at="now")
    assert all(text in md for text in ("Trade Outlook", "Port Watch", "lower freight costs", "increase freight costs", "10 million", "12 million", "did not approve"))
    assert "allegedly" not in md and "unverified claim" not in md


def test_theological_perspective_and_forecast_stay_source_content():
    text="The speaker teaches that trusting Christ rather than worldly power brings courage. The speaker predicts that difficult times will come."
    result=reader_prose(text)
    assert result=="Trusting Christ rather than worldly power brings courage. The speaker predicts that difficult times will come."
    assert not any(word in result for word in ("allegedly", "unsupported", "misleading"))


@pytest.mark.parametrize("first,second", [
    ("The projected cost is 10.","The projected cost is 100."),
    ("The price changed by -10%.","The price changed by +10%."),
    ("The price changed by 3.7%.","The price changed by 3-7%."),
    ("The price changed by −10%.","The price changed by 10%."),
    ("The price is $10.","The price is €10."),
])
def test_distinct_numeric_claims_survive_deduplication(first,second):
    assert reporter._deduplicate_cyberlurch_facts([first,second])==[first,second]


def test_fuller_statement_preserves_a_concrete_detail_instead_of_losing_it_as_a_near_duplicate():
    first="The council approved the harbor project."
    second="The council approved the harbor project on Monday."
    assert reporter._deduplicate_cyberlurch_facts([first,second])==[second]


def response(value):
    return SimpleNamespace(choices=[SimpleNamespace(message=SimpleNamespace(content=json.dumps(value)),finish_reason="stop")])


@pytest.mark.parametrize("language,text", [
    ("en","The proposal could raise freight costs because trucks would travel farther."),
    ("de","Der Vorschlag könnte die Frachtkosten erhöhen, weil Lastwagen weiter fahren müssten."),
    ("sv","Förslaget kan höja fraktkostnaderna eftersom lastbilar måste köra längre."),
])
def test_one_existing_direct_call_supplies_natural_language_and_concrete_effects(monkeypatch,language,text):
    calls=[]
    value={"transcript_full_summary":text,"transcript_key_points":[text],"important_details":[text],
        "transcript_notable_claims":[],"transcript_uncertainties":[],"editorial_relevance":""}
    monkeypatch.setattr(summarizer,"_get_client",lambda:object())
    monkeypatch.setattr(summarizer,"_cyberlurch_chat_create",lambda *a,**k: calls.append(k) or response(value))
    result=summarizer.summarize_youtube_transcript_direct({**row(),"text":text,"language":language})
    assert result["transcript_full_summary"]==text and len(calls)==1
    prompt=calls[0]["messages"][1]["content"]
    assert SOURCE_VOICE_RULE in prompt and "who is affected, what could change and why" in prompt
    assert SOURCE_VOICE_RULE in calls[0]['messages'][0]['content']
    assert "do not wrap each claim" in prompt and "1–2 natural sentences" in prompt
    assert calls[0]["model"]==summarizer.OPENAI_MODEL_CYBERLURCH_DIRECT_DIGEST


@pytest.mark.parametrize('summary,facts', [
    ('Der Beitrag berichtet über landesweite Proteste und eine abgesagte Veranstaltung.',
     ['Die Proteste eskalierten zu Ausschreitungen.', 'Der Umzug wurde wegen zu hoher Sicherheitskosten abgesagt.']),
    ('The transcript discusses concerns about fuel supplies and political consequences.',
     ['Fuel deliveries could cost 20% more because trucks would travel farther.', 'The council has not approved the plan.']),
    ('Inslaget handlar om högre bränslekostnader och politiska följder.',
     ['Bränsleleveranser kan bli dyrare eftersom lastbilarna måste köra längre.']),
])
def test_saved_metadescription_yields_to_concrete_source_sentences_without_calls_or_mutation(summary, facts):
    item=row(transcript_full_summary=summary, transcript_key_points=facts, important_details=[])
    before=copy.deepcopy(item)
    md=reporter.render_cyberlurch_daily_report([item],title='Cyberlurch',generated_at='now')
    snapshot,body=md.split('## Selected items',1)
    assert facts[0] in snapshot and all(fact in body for fact in facts)
    assert summary not in md and item==before


def test_unrewritable_meta_intro_is_not_invented_into_a_claim():
    from newsagent2.cyberlurch_readability import saved_reader_intro
    item=row(transcript_full_summary='The transcript discusses economic implications.', transcript_key_points=[], important_details=[])
    assert saved_reader_intro(item)==''
    assert reader_prose('The transcript specifies that fuel deliveries could cost 20% more.')=='Fuel deliveries could cost 20% more.'


def test_chunk_and_combine_use_same_source_voice_without_new_repair_calls(monkeypatch):
    calls=[]
    monkeypatch.setenv("CYBERLURCH_TRANSCRIPT_CHUNK_CHARS","1000")
    monkeypatch.setenv("CYBERLURCH_TRANSCRIPT_CHUNK_OVERLAP_CHARS","100")
    monkeypatch.setattr(summarizer,"_get_client",lambda:object())
    def create(*a,**kw):
        calls.append(kw)
        if kw['stage']=='chunk': return response({"summary":"Fuel deliveries could become more expensive.","key_points":[],"notable_claims":[],"uncertainties":[]})
        return response({"transcript_full_summary":"Fuel deliveries could become more expensive because trucks would travel farther."})
    monkeypatch.setattr(summarizer,"_cyberlurch_chat_create",create)
    result=summarizer.summarize_youtube_transcript_chunks({**row(),"text":"X"*2600},language="en")
    assert [call['stage'] for call in calls]==['chunk','chunk','chunk','chunk_combine']
    assert all(SOURCE_VOICE_RULE in call['messages'][0]['content'] for call in calls)
    assert result['unique_chars_processed']==2600


def test_monthly_and_yearly_keep_source_positions_with_the_same_editorial_voice():
    for system,_ in (monthly_prompt([],"en"),yearly_prompt([],[],"en")):
        assert SOURCE_VOICE_RULE in system
        assert "without endorsing, rebutting, sanitizing or balancing" in system
        assert "If the source supplies no concrete consequence" in system


def test_weekly_overview_and_detail_use_source_voice_without_assigned_political_tone(monkeypatch):
    calls=[]
    monkeypatch.setenv("REPORT_KEY","cyberlurch")
    monkeypatch.setattr(summarizer,"_get_client",lambda:object())
    def create(*args,**kwargs):
        calls.append(kwargs)
        return SimpleNamespace(choices=[SimpleNamespace(message=SimpleNamespace(content="The plan could increase delivery costs."))])
    monkeypatch.setattr(summarizer,"_openai_chat_create",create)
    item={**row(),"source":"youtube","text":"The plan could increase delivery costs."}
    summarizer.summarize([item],language="en")
    summarizer.summarize_item_detail(item,language="en")
    assert len(calls)==2
    assert all(SOURCE_VOICE_RULE in call["messages"][0]["content"] for call in calls)
    assert "tone_profile" not in calls[1]["messages"][1]["content"]
    assert "Prefer phrasing like: The channel argues" not in calls[0]["messages"][0]["content"]
    assert "fringe/alien/absurd" not in calls[1]["messages"][0]["content"]
