from __future__ import annotations

import pathlib
import sys

SRC = pathlib.Path(__file__).resolve().parents[1] / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from newsagent2.collectors_youtube import build_ytdlp_common_opts


def test_ytdlp_options_default_runtime_is_deno(monkeypatch):
    monkeypatch.delenv("YTDLP_JS_RUNTIME", raising=False)
    monkeypatch.delenv("YTDLP_ALLOW_REMOTE_COMPONENTS", raising=False)
    diag = {}
    opts = build_ytdlp_common_opts(diagnostics=diag)
    assert opts["js_runtimes"] == {"deno": {}}
    assert "remote_components" not in opts
    assert diag["ytdlp_js_runtime_configured"] == "deno"
    assert diag["ytdlp_remote_components_enabled"] is False


def test_ytdlp_options_runtime_override_and_none(monkeypatch):
    monkeypatch.setenv("YTDLP_JS_RUNTIME", "node")
    assert build_ytdlp_common_opts()["js_runtimes"] == {"node": {}}
    monkeypatch.setenv("YTDLP_JS_RUNTIME", "none")
    assert "js_runtimes" not in build_ytdlp_common_opts()


def test_ytdlp_options_remote_components_opt_in(monkeypatch):
    monkeypatch.setenv("YTDLP_ALLOW_REMOTE_COMPONENTS", "1")
    opts = build_ytdlp_common_opts()
    assert opts["remote_components"] == ["ejs:github"]
