"""Direct source perspectives: equal treatment, attribution once, no new calls."""
from copy import deepcopy
from datetime import datetime, timezone
import re

import pytest

from newsagent2 import reporter
from newsagent2.cyberlurch_readability import reader_prose, saved_reader_intro, SOURCE_VOICE_RULE
from newsagent2.cyberlurch_sentences import block_speakers


def speaker_item(channel='Guest Desk'):
    return {'id':'oil','title':'Energy routes and negotiations','url':'https://example.test/oil',
        'channel':channel,'published_at':'2026-10-20T07:00:00Z','content_status':'full_text',
        'text_source':'managed_transcript',
        'transcript_full_summary':'Jordan Vale argues that the negotiations concern access to oil, and he predicts further brinkmanship.',
        'transcript_key_points':[
            'Jordan Vale argues the policy serves a wider strategy to control oil flows.',
            'Jordan Vale contends a full blockade is operationally implausible.',
            'Jordan Vale warns that renewed attacks could occur in six to twelve months.'],
        'important_details':[
            'Jordan Vale states that the country sold about 90% of its oil to China.',
            'Jordan Vale reports that the air force has reportedly lost 17 aircraft in the last 10 days.']}


@pytest.mark.parametrize('deep',[False,True])
@pytest.mark.parametrize('channel',['tagesschau','Darius_official','Guest Desk','DTBM'])
def test_repeated_person_is_identified_once_and_source_perspective_is_direct(channel,deep):
    item=speaker_item(channel);item['cyberlurch_daily_deep_dive']=deep
    before=deepcopy(item)
    md=reporter.render_cyberlurch_daily_report([item],title='Cyberlurch',generated_at='now')
    assert md.count('Jordan Vale')==1 and f'*{channel} · Jordan Vale ·' in md
    assert not re.search(r'Jordan Vale (?:argues|contends|warns|states|reports)',md)
    assert 'The policy serves a wider strategy to control oil flows. A full blockade is operationally implausible.' in md
    assert 'could occur in six to twelve months' in md
    assert '90%' in md and '17 aircraft in the last 10 days' in md and 'reportedly' in md
    assert item==before


def test_same_text_has_same_distance_independent_of_channel_identity():
    left=reporter.render_cyberlurch_daily_report([speaker_item('tagesschau')],title='C',generated_at='now')
    right=reporter.render_cyberlurch_daily_report([speaker_item('Darius_official')],title='C',generated_at='now')
    assert left.replace('tagesschau','SOURCE')==right.replace('Darius_official','SOURCE')


def test_direct_generation_does_not_condition_the_writing_style_on_channel_identity(monkeypatch):
    import json
    from types import SimpleNamespace
    from newsagent2 import summarizer
    calls=[]
    monkeypatch.setattr(summarizer,'_get_client',lambda:object())
    def create(*a,**kwargs):
        calls.append(kwargs)
        return SimpleNamespace(choices=[SimpleNamespace(finish_reason='stop',message=SimpleNamespace(content=json.dumps({
            'transcript_full_summary':'The port could close for six weeks.', 'transcript_key_points':[],
            'important_details':[], 'transcript_notable_claims':[], 'transcript_uncertainties':[]})))])
    monkeypatch.setattr(summarizer,'_cyberlurch_chat_create',create)
    for channel in ('tagesschau','Darius_official'):
        item={**speaker_item(channel),'text':'The port could close for six weeks.','language':'en'}
        result=summarizer.summarize_youtube_transcript_direct(item)
        assert result['transcript_full_summary']=='The port could close for six weeks.' and item['channel']==channel
    assert len(calls)==2 and calls[0]['messages']==calls[1]['messages']
    assert all(c['model']==summarizer.OPENAI_MODEL_CYBERLURCH_DIRECT_DIGEST for c in calls)


def test_a_quoted_politician_is_not_automatically_treated_as_this_blocks_narrator():
    item=speaker_item()
    item['transcript_full_summary']='The council rejected a port proposal after a public hearing.'
    item['transcript_key_points']=['Minister Smith says that the council has approved the plan.']
    assert block_speakers(item)==()
    assert reader_prose(item['transcript_key_points'][0])==item['transcript_key_points'][0]


def test_null_legacy_lists_and_incomplete_names_never_create_a_speaker():
    item=speaker_item();item['transcript_key_points']=None;item['important_details']=None
    assert block_speakers(item)==()


def test_exact_channel_identity_can_establish_a_single_narrator_without_guessing():
    item=speaker_item('Jordan Vale');item['transcript_key_points']=[];item['important_details']=[]
    item['transcript_full_summary']='Jordan Vale states that the port has not reopened.'
    assert block_speakers(item)==('Jordan Vale',)
    assert saved_reader_intro(item)=='The port has not reopened.'
    item['transcript_full_summary']='The speaker argues that the port could close.'
    assert block_speakers(item)==()


@pytest.mark.parametrize('change',[{'content_status':'metadata_only'}, {'source_basis':{'coverage':'none'}}])
def test_unprocessed_metadata_cannot_establish_a_spoken_perspective(change):
    item=speaker_item();item.update(change)
    assert block_speakers(item)==()


@pytest.mark.parametrize('old,new',[
    ('Der Beitrag behauptet, dass das Geschehen orchestriert und kein zufälliges Ereignis ist.',
     'Das Geschehen ist orchestriert und kein zufälliges Ereignis.'),
    ('Der Beitrag behauptet, dass ein Gericht ein Gesetz der Rückführung kippte und so schnelle Rückführungen verhinderte.',
     'Ein Gericht kippte ein Gesetz der Rückführung und verhinderte so schnelle Rückführungen.'),
    ('Der Beitrag behauptet, dass Republikaner angeblich 40 Millionen US-Dollar bereitstellten, um einen Anspruch zu unterstützen.',
     'Republikaner stellten angeblich 40 Millionen US-Dollar bereit, um einen Anspruch zu unterstützen.'),
    ('Der Beitrag behauptet, dass ein orchestrierter Angriff auf Europa stattfindet, ausgelöst durch koordinierte Überfahrten.',
     'Ein orchestrierter Angriff auf Europa findet statt, ausgelöst durch koordinierte Überfahrten.'),
    ('Der Beitrag berichtet, dass das Abkommen die Kosten senken könnte.',
     'Das Abkommen könnte die Kosten senken.'),
    ('Der Beitrag nennt, dass knapp 1000 Einsatzkräfte verletzt worden sein sollen.',
     'Knapp 1000 Einsatzkräfte sollen verletzt worden sein.'),
    ('Der Beitrag zitiert Interviews, wonach Migranten behaupten, sie seien unter Druck gesetzt worden, um weiterzureisen.',
     'Migranten berichten in Interviews, sie seien unter Druck gesetzt worden, um weiterzureisen.'),
    ('Der Beitrag nennt Werbung und andere Informationskampagnen als Mechanismus, um Menschen zur Reise zu bewegen.',
     'Werbung und andere Informationskampagnen dienen als Mechanismus, um Menschen zur Reise zu bewegen.'),
])
def test_german_reporting_frames_are_rewritten_grammatically_without_judging_the_source(old,new):
    assert reader_prose(old)==new
    assert re.findall(r'\d+',old)==re.findall(r'\d+',new)
    for qualifier in ('kein','keine','angeblich','könnte','sollen','seien'):
        assert (qualifier in old)==(qualifier in new)


@pytest.mark.parametrize('text',[
    'Der Beitrag bestreitet, dass das Gericht den Plan gebilligt hat.',
    'Der Beitrag behauptet nicht, dass der Plan funktionieren wird.',
    'Der Beitrag berichtet, dass nicht nur die Stadt, sondern auch der Hafen betroffen ist.',
    'Der Beitrag berichtet, dass der Minister den Entwurf unterzeichnete.',
    'Der Bericht erklärt, dass die Regierung der USA keine Zusage gegeben hat.',
    'Der Beitrag behauptet, dass die Polizei der Menge ein Angebot gemacht hat.',
    'Der Beitrag berichtet, dass die Polizei auf die Menge geschossen hat.',
    'Der Beitrag erklärt, dass ein Minister für Verteidigung eine Zusage gegeben hat.',
    'Der Beitrag berichtet, dass die Stadt sagte: „Wir lehnen das ab.“',
    'Die Staatsanwaltschaft prüft die Vorwürfe gegen das Unternehmen.',
])
def test_denials_quotes_unknown_grammar_and_real_third_party_actions_are_preserved(text):
    assert reader_prose(text)==text


@pytest.mark.parametrize('summary',[
    "The transcript exhorts Christians to receive God's personal love.",
    'The sermon argues that archaeology provides context for the biblical account.',
])
def test_a_transcript_or_sermon_never_replaces_a_human_narrator(summary):
    item=speaker_item();item['transcript_full_summary']=summary
    item['transcript_key_points']=["Knowing God's love brings courage in difficult times.",
        'Archaeology provides geographical and historical context.']
    item['important_details']=[]
    text=saved_reader_intro(item)
    assert summary not in text and item['transcript_key_points'][0] in text
    assert item['transcript_key_points'][1] in text


@pytest.mark.parametrize('language',['en','de'])
@pytest.mark.parametrize('cadence',['daily','weekly','monthly','yearly'])
def test_older_capture_label_is_absent_but_source_metadata_is_retained(language,cadence):
    from test_pubmed_facts import paper
    item=paper(published_at=datetime(2026,10,15,tzinfo=timezone.utc))
    item.update(newly_ingested=True,older_publication=True)
    before=deepcopy(item)
    md=reporter.to_markdown([item],'',{},report_title='The Cybermed Report',report_language=language,report_mode=cadence)
    assert 'Newly captured older publication' not in md and 'Neu erfasste ältere Publikation' not in md
    assert item==before and item['older_publication'] and item['published_at'].date().isoformat()=='2026-10-15'


def test_source_voice_rule_uses_no_channel_trust_classes_or_required_reporting_prefix():
    assert 'same narrative distance for every channel' in SOURCE_VOICE_RULE
    assert 'Name a host or guest once' in SOURCE_VOICE_RULE


def test_source_relationship_is_stated_without_an_invented_causal_claim():
    text='The speaker ties spiritual failure and hostility within the church to three vulnerabilities: legalism, performance and discouragement.'
    assert reader_prose(text)=='Spiritual failure and hostility within the church are linked to three vulnerabilities: legalism, performance and discouragement.'
    assert 'cause' not in reader_prose(text)


def test_a_coherent_two_fact_intro_keeps_five_total_source_points_without_repetitive_bullets():
    item=speaker_item();md=reporter.render_cyberlurch_daily_report([item],title='C',generated_at='now')
    body=md.split('## Selected items',1)[1]
    assert len([line for line in body.splitlines() if line.startswith('- ')])==3
    assert all(token in body for token in ('wider strategy','blockade','six to twelve months','90%','17 aircraft'))


def test_a_long_rewritten_german_intro_can_use_an_existing_short_fact_in_the_snapshot():
    item=speaker_item();item['transcript_full_summary']='Der Beitrag behauptet, dass ein Angriff auf Europa stattfindet, '+('begleitet von weiteren Entwicklungen '*18)+'.'
    item['transcript_key_points']=['Der Beitrag behauptet, dass das Geschehen kein Zufall ist.']
    item['important_details']=[]
    assert saved_reader_intro(item,max_sentences=1,max_words=45)=='Das Geschehen ist kein Zufall.'


def test_weekly_item_blocks_use_the_same_treatment_without_changing_markdown_or_quotes():
    from newsagent2.cyberlurch_readability import reader_block
    item=speaker_item()
    text='### Key takeaways\n\n- Jordan Vale contends the blockade is implausible.\n\n**BOTTOM LINE:** Jordan Vale states that the port has not reopened.\n\n> Jordan Vale says that this is a direct quote.\n\n```\nJordan Vale states that code is literal.\n```'
    actual=reader_block(text,item)
    assert actual.startswith('### Key takeaways\n\n- The blockade is implausible.')
    assert '**BOTTOM LINE:** The port has not reopened.' in actual
    assert '> Jordan Vale says that this is a direct quote.' in actual
    assert 'Jordan Vale states that code is literal.' in actual
    assert text.count('\n')==actual.count('\n')
    md=reporter.to_markdown([item],'## Executive Summary\n\nEnergy negotiations.',{'oil':text},
        report_title='Cyberlurch Weekly',report_language='en',report_mode='weekly')
    assert 'Guest Desk · Jordan Vale' in md and '- The blockade is implausible.' in md
