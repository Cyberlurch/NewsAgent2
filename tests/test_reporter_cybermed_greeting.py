from src.newsagent2.reporter import to_markdown


def test_greeting_only_cybermed(monkeypatch):
    monkeypatch.delenv('REPORT_KEY', raising=False)
    monkeypatch.delenv('REPORT_PROFILE', raising=False)
    monkeypatch.setenv('CYBERMED_SEASONAL_GREETING', '1')
    monkeypatch.setenv('CYBERMED_SEASONAL_GREETING_TEXT', 'Glad midsommar!')
    md = to_markdown([], '', {}, report_title='Cybermed Report', report_language='en')
    assert '*Glad midsommar!*' in md
    md2 = to_markdown([], '', {}, report_title='The Cyberlurch Report', report_language='en')
    assert '*Glad midsommar!*' not in md2
