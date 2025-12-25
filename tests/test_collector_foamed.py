from datetime import datetime, timedelta, timezone

import pytest

from src.newsagent2 import collector_foamed


def _rss_feed(now: datetime, link: str) -> bytes:
    pub_date = now.strftime("%a, %d %b %Y %H:%M:%S GMT")
    return f"""<?xml version="1.0" encoding="UTF-8"?>
<rss version="2.0">
  <channel>
    <title>Example Feed</title>
    <item>
      <title>Post from RSS</title>
      <link>{link}</link>
      <pubDate>{pub_date}</pubDate>
    </item>
  </channel>
</rss>
""".encode()


def test_default_headers_applied_when_missing(monkeypatch):
    captured_headers = []
    feed_url = "https://example.com/feed"
    now = datetime.now(timezone.utc)

    def fake_fetch(session, url, timeout_s=10, headers=None):
        captured_headers.append(headers)
        return collector_foamed._FetchResult(
            ok=True, status_code=200, content=_rss_feed(now, "https://example.com/post"), final_url=url, error=None
        )

    monkeypatch.setattr(collector_foamed, "_fetch_url", fake_fetch)
    monkeypatch.delenv("FOAMED_FORCE_FALLBACK_SOURCES", raising=False)
    monkeypatch.delenv("FOAMED_AUDIT", raising=False)

    items, stats = collector_foamed.collect_foamed_items(
        [
            {
                "name": "Example",
                "feed_url": feed_url,
                "homepage": "",
            }
        ],
        now,
        lookback_hours=24,
    )

    assert items
    assert stats["sources_ok"] == 1
    assert captured_headers, "expected headers to be passed to fetcher"
    applied = captured_headers[0]
    assert applied["User-Agent"] == collector_foamed.DEFAULT_USER_AGENT
    assert "Accept" in applied and "Accept-Language" in applied


def test_force_fallback_sources_skips_rss(monkeypatch):
    requested_urls = []
    feed_url = "https://example.com/feed"
    homepage = "https://example.com/"
    post_url = "https://example.com/post-one"
    now = datetime.now(timezone.utc)
    post_date = now.strftime("%Y-%m-%dT%H:%M:%S%z")

    def fake_fetch(session, url, timeout_s=10, headers=None):
        requested_urls.append(url)
        if url == feed_url:
            pytest.fail("RSS feed should be skipped when forced fallback is enabled")
        if url.rstrip("/") == homepage.rstrip("/"):
            html = f'<html><body><a href="{post_url}">Read</a></body></html>'
            return collector_foamed._FetchResult(ok=True, status_code=200, content=html.encode(), final_url=url, error=None)
        if url.startswith(post_url):
            html = f"""
            <html>
                <head>
                    <meta property="article:published_time" content="{post_date}" />
                    <title>HTML Post</title>
                </head>
                <body><p>Example content</p></body>
            </html>
            """
            return collector_foamed._FetchResult(ok=True, status_code=200, content=html.encode(), final_url=url, error=None)
        return collector_foamed._FetchResult(ok=False, status_code=404, content=None, final_url=url, error=None)

    monkeypatch.setenv("FOAMED_FORCE_FALLBACK_SOURCES", "Example Blog")
    monkeypatch.setattr(collector_foamed, "_fetch_url", fake_fetch)

    items, stats = collector_foamed.collect_foamed_items(
        [
            {
                "name": "Example Blog",
                "feed_url": feed_url,
                "homepage": homepage,
                "max_candidates": 5,
                "max_pages": 3,
            }
        ],
        now,
        lookback_hours=24,
    )

    per_source = stats["per_source"]["Example Blog"]
    assert items and items[0]["url"] == post_url
    assert per_source["html_fallback_used"] is True
    assert per_source["forced_html_fallback"] is True
    assert "feed_ok" not in per_source or not per_source["feed_ok"]
    assert stats["forced_html_fallback_sources"] == ["Example Blog"]
    assert feed_url not in requested_urls


def test_audit_mode_emits_metadata(monkeypatch):
    feed_url = "https://example.com/feed"
    homepage = "https://example.com/"
    rss_url = "https://example.com/from-rss"
    html_url = "https://example.com/from-html"
    now = datetime.now(timezone.utc)
    post_date = (now - timedelta(minutes=10)).strftime("%Y-%m-%dT%H:%M:%S%z")

    def fake_fetch(session, url, timeout_s=10, headers=None):
        if url == feed_url:
            return collector_foamed._FetchResult(ok=True, status_code=200, content=_rss_feed(now, rss_url), final_url=url, error=None)
        if url.rstrip("/") == homepage.rstrip("/"):
            html = f'<html><body><a href="{html_url}">Read</a></body></html>'
            return collector_foamed._FetchResult(ok=True, status_code=200, content=html.encode(), final_url=url, error=None)
        if url.startswith(html_url):
            html = f"""
            <html>
                <head>
                    <meta property="article:published_time" content="{post_date}" />
                    <title>HTML Audit Post</title>
                </head>
                <body><p>Audit content</p></body>
            </html>
            """
            return collector_foamed._FetchResult(ok=True, status_code=200, content=html.encode(), final_url=url, error=None)
        return collector_foamed._FetchResult(ok=False, status_code=404, content=None, final_url=url, error=None)

    monkeypatch.setenv("FOAMED_AUDIT", "1")
    monkeypatch.delenv("FOAMED_FORCE_FALLBACK_SOURCES", raising=False)
    monkeypatch.setattr(collector_foamed, "_fetch_url", fake_fetch)

    items, stats = collector_foamed.collect_foamed_items(
        [
            {
                "name": "Audit Blog",
                "feed_url": feed_url,
                "homepage": homepage,
                "max_candidates": 5,
                "max_pages": 3,
            }
        ],
        now,
        lookback_hours=24,
    )

    assert items  # RSS item should be kept
    audit = stats["per_source"]["Audit Blog"]["audit"]
    assert audit["html_items_in_window"] == 1
    assert audit["rss_items_in_window"] == 1
    assert audit["items_found_in_html_not_in_rss"]["count"] == 1
    assert audit["items_found_in_rss_not_in_html"]["count"] == 1
    assert stats["audit"]["enabled"] is True

    from src.newsagent2 import reporter

    md = reporter.to_markdown(
        [],
        "",
        {},
        report_title="Cybermed Daily",
        report_language="en",
        foamed_stats=stats,
        report_mode="daily",
    )

    assert "html_items_in_window" in md
