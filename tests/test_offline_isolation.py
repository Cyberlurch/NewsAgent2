import json
from pathlib import Path
import subprocess
import sys

import pytest

from _isolation import Policy, IsolationViolation
from newsagent2.run_context import PATHS, path_for


def test_every_registered_path_and_report_is_temporarily_injected(tmp_path):
    for name in PATHS:
        assert path_for(name).is_relative_to(tmp_path)
    import os
    assert Path(os.environ['REPORT_DIR']).is_relative_to(tmp_path)
    assert os.getenv('OPENAI_API_KEY') is None and os.getenv('SMTP_PASS') is None


@pytest.mark.parametrize('event,args',[('socket.connect',()),('socket.getaddrinfo',()),('os.system',('curl invalid',)),
    ('subprocess.Popen',('curl',['curl','invalid'],None,None)),('subprocess.Popen',('python',['python','-Es','-c','pass'],None,None))])
def test_guard_records_attempt_even_when_the_exception_is_caught(tmp_path,event,args):
    policy=Policy(tmp_path/'production',tmp_path/'audit.log')
    try: policy.audit(event,args)
    except IsolationViolation: pass
    assert policy.log.read_text().strip() in {event,'external_process'}


@pytest.mark.parametrize('code',[
    "import socket\ntry: socket.socket().connect(('127.0.0.1',9))\nexcept Exception: pass",
    "import subprocess,sys\nsubprocess.run([sys.executable,'-c',\"import socket\\ntry: socket.socket().connect(('127.0.0.1',9))\\nexcept Exception: pass\"])",
    "import subprocess\ntry: subprocess.run(['curl','https://invalid.example.test'])\nexcept Exception: pass",
])
def test_swallowed_network_or_cli_violation_fails_an_inner_pytest_run(tmp_path,code):
    # The inner pytest plugin owns its own audit log; no network connection or
    # native CLI actually starts. It must fail even when test code catches it.
    probe=tmp_path/'probe'; probe.mkdir()
    (probe/'test_probe.py').write_text('def test_caught():\n'+''.join('    '+line+'\n' for line in code.splitlines()))
    result=subprocess.run([sys.executable,'-m','pytest','-q','-p','conftest',str(probe)],cwd=probe,capture_output=True,text=True)
    assert result.returncode==1, result.stdout+result.stderr
    assert 'Forbidden access was attempted' in result.stdout


def test_protected_write_fails_before_bytes_change_and_snapshot_detects_new_files(tmp_path):
    root=tmp_path/'production'; (root/'state').mkdir(parents=True)
    path=root/'state/store.json'; path.write_bytes(b'{"legacy":1}')
    policy=Policy(root,tmp_path/'audit.log'); before=policy.snapshot()
    import os
    with pytest.raises(IsolationViolation): policy.audit('open',(str(path),'w',os.O_WRONLY|os.O_TRUNC))
    assert path.read_bytes()==b'{"legacy":1}' and policy.snapshot()==before
    (root/'state/new.json').write_text('{}')  # Simulates an unexpected external writer.
    assert policy.snapshot()!=before
