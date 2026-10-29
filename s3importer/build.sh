#!/usr/bin/env bash
# Baut ../build/s3importer.zip (handler.py + meta.py + exifread, pure Python).
set -euo pipefail
cd "$(dirname "$0")"
BUILD="$(cd .. && pwd)/build/s3importer"
rm -rf "${BUILD:?}"
mkdir -p "$BUILD"
cp handler.py meta.py "$BUILD/"
"${PYTHON:-python3}" -m pip install -q --target "$BUILD" "exifread==3.5.1" >/dev/null 2>&1
find "${BUILD:?}" -maxdepth 1 \( -name '*.dist-info' -o -name bin -o -name __pycache__ \) -exec rm -r {} +
rm -f "${BUILD:?}.zip"
( cd "$BUILD" && zip -qr "${BUILD}.zip" . )
echo "${BUILD}.zip"
