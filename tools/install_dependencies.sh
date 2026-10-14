#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")/.."
python -c 'import sys; assert sys.version_info[:2] == (3, 12), "Python 3.12 required"'
case "${1:-runtime}" in
  runtime) requirements=requirements.txt ;;
  dev) requirements=requirements-dev.txt ;;
  *) echo 'usage: install_dependencies.sh [runtime|dev]' >&2; exit 2 ;;
esac
python -m pip install --disable-pip-version-check --no-input -c constraints-python312.txt pip==25.0.1
python -m pip install --disable-pip-version-check --no-input -c constraints-python312.txt -r "$requirements"
python -m pip check
