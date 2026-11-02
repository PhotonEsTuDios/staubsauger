#!/usr/bin/env bash
# Baut den Lambda-Layer photon-ffmpeg (statisches ffmpeg für arm64 -> /opt/bin/ffmpeg) und gibt die Layer-ARN aus.
# Quelle: johnvansickle.com static builds (GPL). Nur einmal nötig.
set -euo pipefail
cd "$(dirname "$0")"
B=${PHOTON_BUCKET:?PHOTON_BUCKET fehlt}
W="$(cd .. && pwd)/build/ffmpeg-layer"
rm -rf "$W"; mkdir -p "$W/bin"
curl -fsSL https://johnvansickle.com/ffmpeg/releases/ffmpeg-release-arm64-static.tar.xz -o "$W/ff.tar.xz"
tar -xJf "$W/ff.tar.xz" -C "$W"
cp "$W"/ffmpeg-*-arm64-static/ffmpeg "$W/bin/ffmpeg"; chmod +x "$W/bin/ffmpeg"
( cd "$W" && zip -q9 layer.zip bin/ffmpeg )
aws s3 cp "$W/layer.zip" "s3://$B/build/ffmpeg-layer.zip" --quiet >&2
aws lambda publish-layer-version --layer-name photon-ffmpeg --compatible-architectures arm64 \
  --content "S3Bucket=$B,S3Key=build/ffmpeg-layer.zip" --query LayerVersionArn --output text
