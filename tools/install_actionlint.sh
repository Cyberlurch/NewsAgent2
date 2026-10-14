#!/usr/bin/env bash
set -euo pipefail
destination="${1:?usage: install_actionlint.sh DESTINATION}"
mkdir -p "$destination"
archive="$destination/actionlint_1.7.12_linux_amd64.tar.gz"
curl --fail --silent --show-error --location --retry 3 \
  https://github.com/rhysd/actionlint/releases/download/v1.7.12/actionlint_1.7.12_linux_amd64.tar.gz -o "$archive"
# Verified against the upstream v1.7.12 release checksums, not an unpinned latest.
echo "8aca8db96f1b94770f1b0d72b6dddcb1ebb8123cb3712530b08cc387b349a3d8  $archive" | sha256sum --check
tar -xzf "$archive" -C "$destination" actionlint
"$destination/actionlint" --version
