#!/usr/bin/env bash
# Baut das Zip für photon-dropbox-ingest (Dropbox-Handler + Raw-Import-Modus) und deployt es.
set -euo pipefail
cd "$(dirname "$0")"
export AWS_PROFILE="${AWS_PROFILE:-photon}" AWS_REGION=eu-west-1
BUILD="$(cd .. && pwd)/build/raw-import"
rm -rf "${BUILD:?}"
mkdir -p "$BUILD"
cp ../dropbox_cloud/handler.py raw_import.py meta.py "$BUILD/"
"${PYTHON:-python3}" -m pip install -q --target "$BUILD" "exifread==3.5.1" >/dev/null
find "${BUILD:?}" -maxdepth 1 \( -name '*.dist-info' -o -name bin \) -exec rm -r {} +
rm -f "${BUILD:?}.zip"
( cd "$BUILD" && zip -qr "${BUILD}.zip" . )
aws lambda update-function-code --function-name photon-dropbox-ingest \
  --zip-file "fileb://${BUILD}.zip" --query '[CodeSize,LastModified]' --output text
