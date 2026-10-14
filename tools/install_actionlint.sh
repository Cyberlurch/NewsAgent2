#!/usr/bin/env bash
set -euo pipefail
destination="${1:?usage: install_actionlint.sh DESTINATION}"
mkdir -p "$destination"
archive="$destination/actionlint_1.7.7_linux_amd64.tar.gz"
curl --fail --silent --show-error --location --retry 3 \
  https://github.com/rhysd/actionlint/releases/download/v1.7.7/actionlint_1.7.7_linux_amd64.tar.gz -o "$archive"
# Verified against the upstream v1.7.7 release checksums, not an unpinned latest.
echo "023070a287cd8cccd71515fedc843f1985bf96c436b7effaecce67290e7e0757  $archive" | sha256sum --check
tar -xzf "$archive" -C "$destination" actionlint
"$destination/actionlint" --version
