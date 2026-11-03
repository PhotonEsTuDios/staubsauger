#!/usr/bin/env bash
# Baut ../build/icloud_cloud.zip (Plan + Worker): Module + meta (s3importer/) + pyicloud für Lambda arm64/py3.12.
set -euo pipefail
cd "$(dirname "$0")"
BUILD="$(cd .. && pwd)/build/icloud_cloud"
rm -rf "${BUILD:?}"; mkdir -p "$BUILD"
cp handler.py session_store.py assets.py matching.py ../s3importer/meta.py "$BUILD/"
"${PYTHON:-python3}" -m pip install -q --target "$BUILD" \
  --platform manylinux_2_28_aarch64 --platform manylinux2014_aarch64 --platform manylinux_2_17_aarch64 \
  --implementation cp --python-version 3.12 --only-binary=:all: \
  "pyicloud==2026.10.1" "exifread==3.5.1" >&2
find "${BUILD:?}" \( -name __pycache__ -o -name tests \) -prune -exec rm -rf {} + 2>/dev/null || true
rm -rf "$BUILD/bin" "$BUILD"/boto3* "$BUILD"/botocore* "$BUILD"/s3transfer*
rm -f "${BUILD:?}.zip"
( cd "$BUILD" && zip -qr "${BUILD}.zip" . )
echo "${BUILD}.zip"
