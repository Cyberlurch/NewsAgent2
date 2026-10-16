"""Run the actual workflow shell loops with a non-sending Python stub.

This isolated control-flow check runs separately from pytest's native-process
ban. It never starts the application, reads credentials or contacts a network.
The child receives an explicit dummy environment and a temporary working tree.
"""
from pathlib import Path
import os
import re
import subprocess
import tempfile

import yaml

ROOT = Path(__file__).resolve().parents[1]


def check():
    scenarios = 0
    for filename, step_name, key in [('cybermed.yml', 'Run Cybermed reports', 'cybermed'),
                                      ('newsagent.yml', 'Run reports', 'cyberlurch')]:
        workflow = yaml.safe_load((ROOT/'.github/workflows'/filename).read_text())
        step = next(s for job in workflow['jobs'].values() for s in job['steps'] if s.get('name') == step_name)
        script = step['run'].replace('${{ github.event_name }}', 'workflow_dispatch')
        if '${{' in script:
            raise RuntimeError('loop_harness_unrecognized_expression')
        # The application command must remain stubbed, including alternate
        # Python executables. New executable operations require harness review.
        if re.search(r'\b(curl|wget|gh|python3|pip|node|deno|docker|git)\b', script):
            raise RuntimeError('loop_harness_unrecognized_command')
        for failed in ['', 'daily', 'weekly', 'monthly', 'yearly']:
            with tempfile.TemporaryDirectory(prefix='newsagent-loop-') as directory:
                root = Path(directory)
                stub = '''python() {
  case "$*" in
    *"-m src.newsagent2.main "*) ;;
    *) return 98 ;;
  esac
  printf '%s:%s\\n' "$REPORT_KEY" "$REPORT_MODE" >> "$MOCK_TRACE"
  if [ "$REPORT_MODE" = "$MOCK_FAIL_MODE" ]; then return 17; fi
  return 0
}
'''
                path = root/'run.sh'; path.write_text(stub+script)
                env = {'PATH': os.defpath, 'EMAIL_MODE': 'none', 'SEND_EMAIL': '0',
                    'REPORT_KEY': key, 'REPORT_DIR': str(root), 'RUN_LOG_PATH': str(root/'run.log'),
                    'RUN_MODES': 'daily weekly monthly yearly', 'RUN_WHICH_REPORT': 'cyberlurch',
                    'MANAGED_TRANSCRIPT_PROVIDER_OVERRIDE': 'none',
                    'MOCK_TRACE': str(root/'trace'), 'MOCK_FAIL_MODE': failed}
                result = subprocess.run(['bash', '--noprofile', '--norc', str(path)],
                    cwd=root, env=env, text=True, capture_output=True, timeout=15)
                trace = (root/'trace').read_text().splitlines() if (root/'trace').exists() else []
                expected = [key+':'+mode for mode in ['daily', 'weekly', 'monthly', 'yearly']]
                if trace != expected or result.returncode != int(bool(failed)):
                    raise RuntimeError('loop_harness_control_flow_failed:'+key+':'+(failed or 'success'))
                scenarios += 1
    print(f'Workflow cadence loops: {scenarios} scenarios passed; all four attempts recorded, overall failures retained.')


if __name__ == '__main__':
    check()
