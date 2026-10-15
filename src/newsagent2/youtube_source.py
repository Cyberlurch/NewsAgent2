"""Source identity and text basis. These are evidence labels, not delivery proof."""
from __future__ import annotations

from dataclasses import asdict, dataclass
from html.parser import HTMLParser
import hashlib
import json
import re
from urllib.parse import unquote, urlsplit

CHANNEL_ID = re.compile(r"UC[A-Za-z0-9_-]{22}\Z")
TRANSCRIPTS = {"managed_transcript", "youtube_transcript_api", "timedtext", "yt_dlp_captions"}


def source_url(value):
    try:
        p = urlsplit(value)
        if p.scheme not in {"http", "https"} or p.hostname not in {"youtube.com", "www.youtube.com", "m.youtube.com"} or p.username or p.password:
            return ""
        path = unquote(p.path).rstrip("/")
        path = re.sub(r"/(?:videos|streams|featured)$", "", path)
        return "https://www.youtube.com" + path
    except (TypeError, ValueError):
        return ""


@dataclass
class Identity:
    status: str = "unresolved"
    channel_id: str = ""
    evidence: str = "none"
    source_url: str = ""

    def public(self):
        return asdict(self)


def configured_identity(channel):
    url = source_url(channel.get("url") or "")
    explicit = str(channel.get("channel_id") or "").strip()
    path_id = url.rsplit("/channel/", 1)[-1] if "/channel/" in url else ""
    if not url or (explicit and not CHANNEL_ID.fullmatch(explicit)) or (path_id and not CHANNEL_ID.fullmatch(path_id)):
        return Identity("conflict", evidence="invalid_configuration", source_url=url)
    if explicit and path_id and explicit != path_id:
        return Identity("conflict", evidence="configured_url_mismatch", source_url=url)
    if explicit or path_id:
        return Identity("configured", explicit or path_id, "operator_id" if explicit else "channel_url", url)
    return Identity(source_url=url)


class _OwnerHTML(HTMLParser):
    def __init__(self):
        super().__init__()
        self.canonical, self.scripts, self._script = [], [], None

    def handle_starttag(self, tag, attrs):
        a = dict(attrs)
        if tag == "link" and a.get("rel") == "canonical":
            self.canonical.append(source_url(a.get("href", "")))
        if tag == "script":
            self._script = ""

    def handle_data(self, data):
        if self._script is not None:
            self._script += data

    def handle_endtag(self, tag):
        if tag == "script" and self._script is not None:
            self.scripts.append(self._script)
            self._script = None


def owner_identity(html, requested_url):
    """Only root ytInitialData.metadata.channelMetadataRenderer, never cards."""
    target = source_url(requested_url)
    parser = _OwnerHTML()
    parser.feed(html[:2_000_000])
    owners = []
    for script in parser.scripts:
        match = re.search(r'(?:var\s+ytInitialData|window\["ytInitialData"\]|ytInitialData)\s*=\s*', script)
        if not match:
            continue
        try:
            obj, _ = json.JSONDecoder().raw_decode(script[match.end():].lstrip())
            owner = obj.get("metadata", {}).get("channelMetadataRenderer", {})
            cid = owner.get("externalId", "")
            urls = {source_url(owner.get(k, "")) for k in ("channelUrl", "vanityChannelUrl")}
            if CHANNEL_ID.fullmatch(cid) and target in urls:
                owners.append(cid)
        except (ValueError, TypeError, AttributeError):
            continue
    ids = set(owners)
    canonical_ids = {u.rsplit("/channel/", 1)[-1] for u in parser.canonical if "/channel/" in u}
    if len(ids) > 1 or (ids and canonical_ids and canonical_ids != ids):
        return Identity("conflict", evidence="owner_metadata_mismatch", source_url=target)
    if len(ids) == 1:
        return Identity("resolved", ids.pop(), "owner_metadata", target)
    return Identity(source_url=target)


class Listing(list):
    def __init__(self, items=(), *, status="unknown", identity=None, raw_count=0, http_status=None):
        super().__init__(items)
        self.status, self.identity = status, identity or Identity()
        self.raw_count, self.http_status = raw_count, http_status


def listing_valid(items):
    return getattr(items, "status", "") in {"items", "empty_valid", "outside_window"} or (bool(items) and not isinstance(items, Listing))


def note_listing(diag, items):
    for key, value in (("youtube_listing_status_counts", getattr(items, "status", "items" if items else "unknown")),
                       ("youtube_identity_status_counts", getattr(items, "identity", Identity()).status)):
        counts = diag.setdefault(key, {})
        counts[value] = counts.get(value, 0) + 1


def bound_cache_key(channel):
    return hashlib.sha256(source_url(channel.get("url", "")).encode()).hexdigest()


def cached_identity(cache, channel):
    local = configured_identity(channel)
    row = cache.get("bindings", {}).get(bound_cache_key(channel), {})
    if local.status == "configured" and row and row.get("channel_id") != local.channel_id:
        return Identity("conflict", evidence="cache_configuration_mismatch", source_url=local.source_url)
    if local.status != "unresolved":
        return local
    if row and row.get("source_url") == local.source_url:
        return Identity(**row)
    if channel.get("name") in cache.get("channels", {}):
        return Identity("legacy_unverified", evidence="legacy_name_cache", source_url=local.source_url)
    return local


def cache_identity(cache, identity):
    if identity.status not in {"resolved", "configured"}:
        return
    bindings = cache.setdefault("bindings", {})
    key = hashlib.sha256(identity.source_url.encode()).hexdigest()
    if key in bindings and bindings[key]["channel_id"] != identity.channel_id:
        return  # Never overwrite a conflicting binding; caller holds the source.
    if len(bindings) < 256 or key in bindings:
        bindings[key] = identity.public()


def identity_valid(row):
    try:
        i = Identity(**row)
        return (i.status in {"unresolved", "resolved", "configured", "conflict", "legacy_unverified"}
                and isinstance(i.channel_id, str) and (not i.channel_id or bool(CHANNEL_ID.fullmatch(i.channel_id)))
                and (i.status not in {"configured", "resolved"} or bool(i.channel_id))
                and (i.status != "configured" or i.evidence in {"operator_id", "channel_url"})
                and (i.status != "resolved" or i.evidence in {"for_handle", "owner_metadata"})
                and isinstance(i.evidence, str) and bool(re.fullmatch(r"[a-z_]{1,50}", i.evidence))
                and isinstance(i.source_url, str) and len(i.source_url) <= 1000
                and (not i.source_url or source_url(i.source_url) == i.source_url))
    except (TypeError, ValueError):
        return False


def cache_valid(value):
    if not isinstance(value, dict) or not isinstance(value.get("channels"), dict):
        return False
    bindings = value.get("bindings", {})
    if not isinstance(bindings, dict) or len(bindings) > 256:
        return False
    for key, row in bindings.items():
        try:
            if not identity_valid(row):
                return False
            identity = Identity(**row)
            if (identity.status not in {"configured", "resolved"} or not CHANNEL_ID.fullmatch(identity.channel_id)
                    or not identity.source_url or source_url(identity.source_url) != identity.source_url
                    or key != hashlib.sha256(identity.source_url.encode()).hexdigest()):
                return False
        except (TypeError, ValueError):
            return False
    return True


def basis(provider="", text=""):
    return {"version": 1, "kind": "transcript" if provider in TRANSCRIPTS else ("description" if provider == "description" else "metadata_only"),
            "provider": provider, "available_chars": len(text), "processed_unique_chars": 0,
            "coverage": "none", "video_completeness": "unknown"}


def stored_basis(item):
    value = item.get("source_basis")
    return dict(value) if basis_valid(value) else {"version": 1, "kind": "unknown", "provider": "legacy_unknown",
        "available_chars": 0, "processed_unique_chars": 0, "coverage": "unknown", "video_completeness": "unknown"}


def basis_valid(value):
    return (isinstance(value, dict) and value.get("version") == 1
            and value.get("kind") in ("transcript", "description", "metadata_only", "unknown")
            and value.get("coverage") in ("none", "partial", "available_text", "unknown")
            and value.get("video_completeness") == "unknown" and isinstance(value.get("provider"), str)
            and all(type(value.get(k)) is int and value[k] >= 0 for k in ("available_chars", "processed_unique_chars"))
            and value["processed_unique_chars"] <= value["available_chars"])


def digest_valid(value):
    from .store_io import digest_store
    return (digest_store(value)
            and all(("source_basis" not in row or basis_valid(row["source_basis"]))
                    and ("channel_identity" not in row or identity_valid(row["channel_identity"])) for row in value["digests"]))


def basis_label(item):
    b = stored_basis(item)
    names = {"description": "Description", "transcript": "Transcript", "metadata_only": "Metadata only", "unknown": "Text basis unknown (legacy)"}
    coverage = {"available_text": "available text processed", "partial": "partial processing", "none": "summary unavailable", "unknown": "coverage unknown"}
    return names[b["kind"]] + " · " + coverage[b["coverage"]]


def apply_basis(item, result):
    b = stored_basis(item)
    b["processed_unique_chars"] = min(b["available_chars"], int(result.get("unique_chars_processed", 0)))
    b["coverage"] = ("unknown" if b["kind"] == "unknown" else
        "available_text" if b["available_chars"] and b["processed_unique_chars"] == b["available_chars"] else
        "partial" if b["processed_unique_chars"] else "none")
    item["source_basis"] = b
