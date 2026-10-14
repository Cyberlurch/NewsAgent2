# Offline development and CI

Use Python 3.12 on Linux. No SMTP, model, transcript-provider, PubMed or GitHub
credentials are required for tests. Package/tool installation uses the network;
the normal test suite blocks it unconditionally. There are currently no live
integration tests and no unit tests have been relabelled as integration tests.

## Fresh installation and checks

From the repository root:

```bash
python3.12 -m venv /tmp/newsagent-dev
source /tmp/newsagent-dev/bin/activate
bash tools/install_dependencies.sh dev
bash tools/install_actionlint.sh /tmp/newsagent-actionlint
```

The installer pins pip and uses `constraints-python312.txt` for both runtime
and development packages, then runs `pip check`. Production and maintenance
workflows use `bash tools/install_dependencies.sh runtime`; pytest and PyYAML
are development-only requirements. The lock includes direct and transitive
dependencies and preserves the already tested environment's versions.
Actionlint 1.7.7 is downloaded from its upstream release and SHA-256 verified.

Run checks in a separate filesystem copy when auditing state integrity. This
does not require a Git worktree. For example, after activating the environment:

```bash
export NEWSAGENT_TEST_COPY=$(mktemp -d /tmp/newsagent-check.XXXXXX)
python - <<'PY'
import os, shutil
from pathlib import Path
shutil.copytree(Path.cwd(), Path(os.environ['NEWSAGENT_TEST_COPY']) / 'repo',
    ignore=shutil.ignore_patterns('.git', '.venv', '__pycache__', '.pytest_cache', '.ci-results'))
PY
cd "$NEWSAGENT_TEST_COPY/repo"
export PYTHONPATH=src
python -m compileall -q src tests tools
python -c 'from newsagent2 import main, emailer, delivery, pre_smtp_recovery, run_context'
python tools/verify_configs.py
/tmp/newsagent-actionlint/actionlint -shellcheck="" -pyflakes=""
bash -n tools/install_dependencies.sh tools/install_actionlint.sh
python -m pytest -q tests/test_youtube_rss_fallback.py tests/test_delivery_boundary.py tests/test_pre_smtp_recovery.py tests/test_offline_isolation.py tests/test_configuration_validation.py tests/test_youtube_content_providers.py tests/test_managed_transcript_provider.py tests/test_backfill_audit.py
python -m pytest -q --junitxml=.ci-results/pytest.xml
git -C /workspace/NewsAgent2 diff --check  # Use your original checkout path.
```

Do not run a newsletter CLI or workflow dispatch as a test. Use mocks with
explicit dummy recipients; a missing mock is an error, even when application
code catches the blocked call. No environment switch disables this protection.

## Isolation and integrity evidence

Before test-module imports, `conftest.py` removes inherited recipient/SMTP/model
settings and credential variables, sets `EMAIL_MODE=none`/`SEND_EMAIL=0`, and
injects every registered `run_context.PATHS` destination into a session temp dir.
Each test receives its own temporary paths and may deliberately override them.
The registry covers processed items (including FOAMed health), rollups, both
Cybermed stores, Cyberlurch digests, YouTube channel IDs/content/attempts and the
delivery journal. The writer lock is derived from the injected journal;
`REPORT_DIR` isolates reports, diagnostics, local state and recovery receipts.
Provider-local temporaries also use the test temp directory.

Saved-store parity/sanitization tests get byte-identical temporary copies of
two explicitly named existing stores. Only the pre-test snapshot step reads
those source stores; tests do not use their production paths. Configuration
files remain readable. Source-store reads and all mutations below `state/`,
`data/`, `out/`, `reports/`, `health/`, `.cache/` are otherwise guarded. `.env`
and a private recipient file cannot be used as credential fallbacks.

A process-wide audit hook blocks socket activity and external native processes
before the operation executes. Python CLI children inherit it through
`sitecustomize`, even with a replaced `PYTHONPATH`; Python startup options that
disable that bootstrap are denied. urllib3's import-time IPv6 probe is replaced
by a fixed test capability, rather than allowing it to bind a socket. Audit
logs contain event categories, not connection arguments or credentials.
Caught attempts still fail test teardown or the collection/session check.
Regression probes verify caught socket, nested Python and native CLI attempts.

`.ci-results/integrity.json` compares SHA-256 inventories before and after the
suite, including directories and new untracked filenames. Any difference or
session violation changes the exit status to failure; nothing restores or
deletes unexpected production files to hide it. JUnit and the hash manifest are
the only CI artifacts; production stores and test temp state are not uploaded.

## Controlled dependency updates

Regenerate with the current pins in a fresh environment:

```bash
python tools/update_dependency_lock.py --output /tmp/constraints-reviewed.txt
diff -u constraints-python312.txt /tmp/constraints-reviewed.txt
```

The updater keeps every existing constraint unless explicitly allowlisted.
For a deliberate provider-library update:

```bash
python tools/update_dependency_lock.py \
  --allow-update yt-dlp --allow-update youtube-transcript-api \
  --output /tmp/constraints-reviewed.txt
diff -u constraints-python312.txt /tmp/constraints-reviewed.txt
```

Review upstream compatibility and the version diff before adopting it. If a
dependency needs a transitive update, name that package explicitly as well;
there is no blanket unlock. Direct pins in `requirements-dev.txt` must be edited
explicitly when updating test tools. Adopt the reviewed file, create new runtime
and dev venvs, run the shared installers and `pip check` in both, then the targeted
provider/RSS/recovery tests and complete offline checks above. Compare shared
installed runtime versions. Preserve model names/budgets. No paid/live provider
test is necessary for a dependency update in this workflow.

## CI and configuration scope

`Offline CI` runs on pull requests and pushes to `main`, with `contents: read`,
its own concurrency group, no production secrets, and normal checkout of the
PR merge ref. All validation/test failures fail the job. The six production
workflows retain their writer locks, refs, permissions and recovery behavior;
only their common dependency installation changes.

Actionlint explicitly disables optional host-provided ShellCheck/Pyflakes so
its checks are identical on local machines and hosted runners. Workflow syntax,
expressions and action contracts remain checked; Python compile/import checks
and installer Bash syntax checks run separately. A local optional ShellCheck
0.10.0 pass found five existing hints in production workflow scripts (SC2086,
SC2129, SC2034); these are outside the allowed dependency-only workflow edits
and are not included in the required check.

The validator checks duplicate keys recursively in the six declared JSON
configuration files and all workflow YAML, plus small existing reader
contracts. Actionlint checks workflow semantics. Historical newsletter stores,
correction records and legacy `data/processed_items.json` are not assigned a new
schema. The duplicate `tier3_pain_strict` entry was removed while retaining the
previous effective last-key-wins list: `Pain`, `Reg Anesth Pain Med`.

After a successful real GitHub run, an operator may make
**`Offline tests (Python 3.12)`** required in the `main` ruleset or branch
protection under required status checks. No branch protection change or new
secret is made by this PR. Local success alone does not prove a GitHub run.
