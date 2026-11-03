#!/usr/bin/env bash
# Baut ../build/faces_cloud.zip für Lambda arm64/py3.12: Module + Modelle (feste Version, Prüfsumme) + OpenCV/numpy/Pillow.
# Lädt fehlende Modelle auch für die lokalen Tests nach faces_cloud/models/ (nicht im Git).
set -euo pipefail
cd "$(dirname "$0")"
ZOO=48b10fa505aeb51fbd127fc60a5c660040e823b7   # opencv_zoo-Commit
MODELS=(
  "face_detection_yunet/face_detection_yunet_2023mar.onnx 8f2383e4dd3cfbb4553ea8718107fc0423210dc964f9f4280604804ed2552fa4"
  "face_recognition_sface/face_recognition_sface_2021dec.onnx 0ba9fbfa01b5270c96627c4ef784da859931e02f04419c829e83484087c34e79"
)
mkdir -p models
for m in "${MODELS[@]}"; do
  path=${m% *}; sum=${m#* }; f=models/$(basename "$path")
  [ -f "$f" ] || curl -sfL -o "$f" "https://github.com/opencv/opencv_zoo/raw/$ZOO/models/$path"
  echo "$sum  $f" | shasum -a 256 -c --quiet - || { echo "✗ Prüfsumme falsch: $f" >&2; rm -f "$f"; exit 1; }
done
[ "${MODELS_ONLY:-0}" = 1 ] && exit 0

BUILD="$(cd .. && pwd)/build/faces_cloud"
rm -rf "${BUILD:?}"; mkdir -p "$BUILD/models"
cp handler.py detect.py cluster.py people.py store.py "$BUILD/"
cp models/*.onnx "$BUILD/models/"
"${PYTHON:-python3}" -m pip install -q --target "$BUILD" \
  --platform manylinux_2_28_aarch64 --platform manylinux2014_aarch64 --platform manylinux_2_17_aarch64 \
  --implementation cp --python-version 3.12 --only-binary=:all: \
  "opencv-python-headless==4.11.0.86" "numpy==2.2.6" "pillow==11.3.0" >&2
find "${BUILD:?}" \( -name '*.dist-info' -o -name __pycache__ -o -name tests \) -prune -exec rm -rf {} + 2>/dev/null || true
rm -rf "$BUILD/bin"
rm -f "${BUILD:?}.zip"
( cd "$BUILD" && zip -qr "${BUILD}.zip" . )
echo "  entpackt $(du -sm "$BUILD" | cut -f1) MB (Grenze 250)" >&2
echo "${BUILD}.zip"
