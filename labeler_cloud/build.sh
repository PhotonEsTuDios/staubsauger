#!/usr/bin/env bash
# Baut ../build/labeler_cloud.zip: handler + quality (labeler/) + meta (s3importer/) + Pillow/numpy/exifread für Lambda arm64.
set -euo pipefail
cd "$(dirname "$0")"
BUILD="$(cd .. && pwd)/build/labeler_cloud"
rm -rf "${BUILD:?}"; mkdir -p "$BUILD"
cp handler.py ../labeler/quality.py ../s3importer/meta.py "$BUILD/"
"${PYTHON:-python3}" -m pip install -q --target "$BUILD" \
  --platform manylinux_2_28_aarch64 --platform manylinux2014_aarch64 \
  --implementation cp --python-version 3.12 --only-binary=:all: \
  "pillow==11.3.0" "numpy==2.2.6" "exifread==3.5.1" >&2
find "${BUILD:?}" \( -name '*.dist-info' -o -name __pycache__ -o -name tests \) -prune -exec rm -rf {} + 2>/dev/null || true
rm -rf "$BUILD/bin"
rm -f "${BUILD:?}.zip"
( cd "$BUILD" && zip -qr "${BUILD}.zip" . )
echo "${BUILD}.zip"
