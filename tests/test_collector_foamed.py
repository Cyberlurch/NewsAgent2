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

    assert "html_items_in_window" not in md
    assert audit["content_mode"] in {"rss_excerpt", "rss_full_content", "rss_title_only", "html_content", "html_excerpt", "no_recent_content", "unavailable"}
    assert isinstance(audit["completeness_warning"], list)


@pytest.mark.parametrize(
    "text_len,expected_mode",
    [
        (10, "rss_title_only"),
        (120, "rss_excerpt"),
        (900, "rss_full_content"),
    ],
)
def test_content_mode_classifies_rss(monkeypatch, text_len, expected_mode):
    now = datetime.now(timezone.utc)
    feed_url = "https://example.com/feed"
    body = "x" * text_len
    pub_date = now.strftime("%a, %d %b %Y %H:%M:%S GMT")
    rss = f"""<rss><channel><item><title>T</title><link>https://example.com/p</link><pubDate>{pub_date}</pubDate><description>{body}</description></item></channel></rss>""".encode()
    monkeypatch.setenv("FOAMED_AUDIT", "1")
    monkeypatch.setattr(collector_foamed, "_fetch_url", lambda *a, **k: collector_foamed._FetchResult(ok=True, status_code=200, content=rss, final_url=feed_url, error=None))
    _, stats = collector_foamed.collect_foamed_items([{"name": "S", "feed_url": feed_url, "homepage": "https://example.com"}], now, 24)
    assert stats["per_source"]["S"]["content_mode"] == expected_mode


def test_article_fetch_improves_excerpt_and_sets_trafilatura(monkeypatch):
    now = datetime.now(timezone.utc)
    feed_url = "https://example.com/feed"
    post = "https://example.com/p"
    pub_date = now.strftime("%a, %d %b %Y %H:%M:%S GMT")
    rss = f"""<rss><channel><item><title>T</title><link>{post}</link><pubDate>{pub_date}</pubDate><description>short</description></item></channel></rss>""".encode()
    monkeypatch.setenv("FOAMED_ARTICLE_FETCH", "1")
    monkeypatch.setenv("FOAMED_ARTICLE_FETCH_MAX_PER_RUN", "25")
    monkeypatch.setattr(collector_foamed, "_extract_article_text", lambda html: ("X" * 900, "trafilatura"))
    def fake_fetch(_s, url, **_k):
        if url == feed_url:
            return collector_foamed._FetchResult(True, 200, rss, url, None)
        return collector_foamed._FetchResult(True, 200, b"<html><body>ok</body></html>", url, None)
    monkeypatch.setattr(collector_foamed, "_fetch_url", fake_fetch)
    items, stats = collector_foamed.collect_foamed_items([{"name": "S", "feed_url": feed_url, "homepage": "https://example.com"}], now, 24)
    assert items[0]["content_source"] == "article_full_text"
    assert items[0]["extraction_method"] == "trafilatura"
    assert stats["foamed_article_fetch_improved_text_total"] == 1


def test_article_fetch_blocked_timeout_ssl_classification(monkeypatch):
    now = datetime.now(timezone.utc)
    feed_url = "https://example.com/feed"
    pub_date = now.strftime("%a, %d %b %Y %H:%M:%S GMT")
    rss = f"""<rss><channel><item><title>A</title><link>https://example.com/a</link><pubDate>{pub_date}</pubDate></item><item><title>B</title><link>https://example.com/b</link><pubDate>{pub_date}</pubDate></item><item><title>C</title><link>https://example.com/c</link><pubDate>{pub_date}</pubDate></item></channel></rss>""".encode()
    monkeypatch.setenv("FOAMED_ARTICLE_FETCH", "1")
    seq = {"n": 0}
    def fake_fetch(_s, url, **_k):
        if url == feed_url:
            return collector_foamed._FetchResult(True, 200, rss, url, None)
        seq["n"] += 1
        if seq["n"] == 1:
            return collector_foamed._FetchResult(False, 403, b"", url, None)
        if seq["n"] == 2:
            return collector_foamed._FetchResult(False, None, None, None, "request_exception:ConnectTimeout")
        return collector_foamed._FetchResult(False, None, None, None, "request_exception:SSLError")
    monkeypatch.setattr(collector_foamed, "_fetch_url", fake_fetch)
    items, stats = collector_foamed.collect_foamed_items([{"name": "S", "feed_url": feed_url, "homepage": "https://example.com"}], now, 24)
    assert len(items) == 3
    assert stats["foamed_article_fetch_blocked_total"] == 1
    assert stats["foamed_article_fetch_timeout_total"] == 1
    assert stats["foamed_article_fetch_ssl_error_total"] == 1
def test_global_counts_and_content_source_semantics(monkeypatch):
    now = datetime.now(timezone.utc)
    feed = "https://example.com/feed"
    post1 = "https://example.com/p1"
    post2 = "https://example.com/p2"
    pub = now.strftime("%a, %d %b %Y %H:%M:%S GMT")
    rss = f"""<rss><channel>
    <item><title>A</title><link>{post1}</link><pubDate>{pub}</pubDate><description>short</description></item>
    <item><title>B</title><link>{post2}</link><pubDate>{pub}</pubDate><description>{'x'*900}</description></item>
    </channel></rss>""".encode()
    monkeypatch.setenv("FOAMED_ARTICLE_FETCH", "1")
    def fake_extract(_html):
        return ("X"*1000, "trafilatura")
    monkeypatch.setattr(collector_foamed, "_extract_article_text", fake_extract)
    monkeypatch.setattr(collector_foamed, "_fetch_url", lambda _s, url, **_k: collector_foamed._FetchResult(True, 200, rss if url == feed else b"<html></html>", url, None))
    items, stats = collector_foamed.collect_foamed_items([{"name": "S", "feed_url": feed, "homepage": "https://example.com"}], now, 24)
    assert len(items) == 2
    assert stats["foamed_article_extraction_method_counts"]["trafilatura"] >= 1
    assert stats["foamed_content_source_counts"]["article_full_text"] >= 1
    assert (
        stats["foamed_discovery_content_mode_counts"].get("rss_excerpt", 0)
        + stats["foamed_discovery_content_mode_counts"].get("rss_title_only", 0)
        + stats["foamed_discovery_content_mode_counts"].get("rss_full_content", 0)
    ) >= 1
    assert stats["foamed_final_content_source_counts"]["article_full_text"] >= 1
    assert items[0]["discovery_content_mode"] in {"rss_excerpt", "rss_full_content", "rss_title_only"}
    assert items[0]["final_content_source"] == items[0]["content_source"]


def test_status_classifications(monkeypatch):
    assert collector_foamed._source_status_from({"feed_status_code": 403, "homepage_status_code": 403, "blocked": True, "content_mode": "unavailable", "candidates_found": 0}, has_recent_items=False, strategy="rss_then_article", audit_only=False) == "blocked"
    assert collector_foamed._source_status_from({"feed_status_code": 404, "homepage_status_code": 404, "content_mode": "unavailable", "candidates_found": 0}, has_recent_items=False, strategy="rss_then_article", audit_only=False) == "stale_or_broken_url"
    assert collector_foamed._source_status_from({"feed_status_code": 403, "homepage_status_code": 200, "content_mode": "html_excerpt", "candidates_found": 3}, has_recent_items=True, strategy="rss_then_article", audit_only=False) == "usable_discovery_only"
    assert collector_foamed._source_status_from({"feed_status_code": 404, "homepage_status_code": 200, "content_mode": "html_content", "candidates_found": 3}, has_recent_items=True, strategy="rss_then_article", audit_only=False) == "usable_html_only"
    assert collector_foamed._source_status_from({"error": "request_exception:ConnectTimeout"}, has_recent_items=False, strategy="rss_then_article", audit_only=False) == "tls_or_timeout_problem"
    assert collector_foamed._source_status_from({"feed_status_code": 403, "homepage_status_code": 200, "content_mode": "html_excerpt", "candidates_found": 2}, has_recent_items=False, strategy="rss_then_article", audit_only=True) == "audit_only"
    assert collector_foamed._source_status_from({"feed_status_code": 404, "homepage_status_code": 200, "content_mode": "html_content", "candidates_found": 2}, has_recent_items=True, strategy="rss_then_article", audit_only=True) == "usable_html_only"
    assert collector_foamed._source_status_from({"feed_status_code": 403, "homepage_status_code": 403, "content_mode": "unavailable", "candidates_found": 0}, has_recent_items=False, strategy="rss_then_article", audit_only=True) == "blocked"
    assert collector_foamed._source_status_from({"feed_status_code": 404, "homepage_status_code": 404, "content_mode": "unavailable", "candidates_found": 0}, has_recent_items=False, strategy="rss_then_article", audit_only=True) == "stale_or_broken_url"


def test_disabled_source_alternative_paths_and_modes(monkeypatch):
    now = datetime.now(timezone.utc)
    monkeypatch.setenv("FOAMED_AUDIT", "1")
    scenarios = [
        ("JournalFeed", 403, 200, 2, "html_excerpt", "feed_blocked_but_html_ok", {"usable_html_only", "audit_only"}),
        ("Critical Care Reviews", 404, 200, 1, "html_content", "feed_broken_but_html_ok", {"usable_html_only", "audit_only"}),
        ("ALiEM", 403, 403, 0, "unavailable", "none", {"blocked"}),
        ("REBEL", 404, 404, 0, "unavailable", "none", {"stale_or_broken_url"}),
    ]
    for name, feed_code, home_code, cands, mode, alt, statuses in scenarios:
        per = {
            "items_raw": 0, "items_with_date": 0, "items_date_unknown": 0, "kept_last24h": 0, "errors": 0,
            "feed_ok": False, "feed_failed": True, "discovered_feed_used": False, "html_fallback_used": False,
            "method": "html_fallback", "why": "feed_failed", "health": "other", "entries_total": 0, "entries_with_date": 0,
            "newest_entry_datetime": None, "error": f"feed_http_{feed_code}", "candidates_found": cands, "pages_fetched": 0,
            "pages_with_date": 0, "blocked": feed_code == 403 and home_code == 403, "feed_status_code": feed_code,
            "homepage_status_code": home_code, "discovered_feed_url": None, "forced_html_fallback": False, "audit": None,
            "content_mode": mode, "discovery_content_mode": mode, "final_content_source": mode, "source_status": "no_recent_content",
            "source_strategy": "rss_then_article", "article_fetch_attempted": 0, "article_fetch_success": 0, "article_fetch_failed": 0,
            "article_fetch_improved_text": 0, "article_fetch_blocked": 0, "article_fetch_timeout": 0, "article_fetch_ssl_error": 0,
            "extraction_method_counts": {}, "content_source_counts": {}, "median_article_text_length": 0, "median_final_text_length": 0,
            "wp_rest_available": False, "wp_rest_items_seen": 0, "wp_rest_items_in_window": 0,
            "sitemap_available": False, "sitemap_items_seen": 0, "sitemap_items_in_window": 0, "alternative_path": alt,
        }
        assert per["alternative_path"] == alt
        status = collector_foamed._source_status_from(per, has_recent_items=False, strategy="rss_then_article", audit_only=True)
        assert status in statuses


def test_html_only_handles_feed_failure_and_returns_items(monkeypatch):
    now = datetime.now(timezone.utc)
    homepage = "https://example.com/"
    post = "https://example.com/h1"
    post_date = now.strftime("%Y-%m-%dT%H:%M:%S%z")
    cfg = [{"name": "JournalFeed (Critical Care)", "feed_url": "https://example.com/feed", "homepage": homepage, "extraction_strategy": "html_only"}]

    def fake_fetch(_s, url, **_k):
        if "feed" in url:
            return collector_foamed._FetchResult(False, 403, b"", url, None)
        if url.rstrip("/") == homepage.rstrip("/"):
            return collector_foamed._FetchResult(True, 200, f'<a href="{post}">x</a>'.encode(), url, None)
        return collector_foamed._FetchResult(True, 200, f'<meta property="article:published_time" content="{post_date}"/><p>hi</p>'.encode(), url, None)

    monkeypatch.setenv("FOAMED_ARTICLE_FETCH", "1")
    monkeypatch.setenv("FOAMED_ARTICLE_FETCH_MAX_PER_RUN", "25")
    monkeypatch.setattr(collector_foamed, "_extract_article_text", lambda _h: ("Y" * 900, "trafilatura"))
    monkeypatch.setattr(collector_foamed, "_fetch_url", fake_fetch)
    items, stats = collector_foamed.collect_foamed_items(cfg, now, 24)
    assert len(items) == 1
    assert items[0]["final_content_source"] == "article_full_text"
    assert items[0]["extraction_method"] == "trafilatura"
    s = stats["per_source"]["JournalFeed (Critical Care)"]
    assert s["source_strategy"] == "html_only"
    assert s["feed_status_code"] is None
    assert s["homepage_status_code"] == 200
    assert s["article_fetch_attempted"] > 0
    assert s["article_fetch_success"] > 0
    assert stats["foamed_html_only_article_fetch_attempted_total"] > 0
    assert stats["foamed_html_only_article_fetch_success_total"] > 0
    assert s["source_status"] in {"usable_html_only", "usable_fulltext"}


def test_audit_only_returns_no_items(monkeypatch):
    now = datetime.now(timezone.utc)
    cfg = [{"name": "ALiEM", "feed_url": "https://example.com/feed", "homepage": "https://example.com", "extraction_strategy": "audit_only"}]
    monkeypatch.setattr(collector_foamed, "_fetch_url", lambda *_a, **_k: collector_foamed._FetchResult(False, 403, b"", "u", None))
    items, stats = collector_foamed.collect_foamed_items(cfg, now, 24)
    assert items == []
    assert stats["foamed_strategy_items_audit_only_total"] >= 0
    serialized = str(stats)
    for banned in ["raw_html", "SMTP_PASS", "OPENAI_API_KEY", "RECIPIENTS_CONFIG_JSON", "@"]:
        assert banned not in serialized


def test_html_only_fetch_runs_before_state_filtering_audit_shape(monkeypatch):
    now = datetime.now(timezone.utc)
    cfg = [{"name": "JournalFeed (Critical Care)", "feed_url": "https://example.com/feed", "homepage": "https://example.com/", "extraction_strategy": "html_only", "force_html_fallback": True}]
    post = "https://example.com/post-1"
    post_date = now.strftime("%Y-%m-%dT%H:%M:%S%z")

    def fake_fetch(_s, url, **_k):
        if "feed" in url:
            return collector_foamed._FetchResult(False, 403, b"", url, None)
        if url.rstrip("/") == "https://example.com":
            return collector_foamed._FetchResult(True, 200, f'<a href="{post}">x</a>'.encode(), url, None)
        return collector_foamed._FetchResult(True, 200, f'<meta property="article:published_time" content="{post_date}"/><p>tiny</p>'.encode(), url, None)

    monkeypatch.setenv("FOAMED_ARTICLE_FETCH", "1")
    monkeypatch.setattr(collector_foamed, "_extract_article_text", lambda _h: ("Z" * 650, "trafilatura"))
    monkeypatch.setattr(collector_foamed, "_fetch_url", fake_fetch)
    items, stats = collector_foamed.collect_foamed_items(cfg, now, 24)
    assert len(items) == 1
    assert stats["per_source"]["JournalFeed (Critical Care)"]["article_fetch_attempted"] == 1
    assert stats["foamed_html_only_article_fetch_improved_text_total"] == 1
