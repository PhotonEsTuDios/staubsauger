"""Gesichter finden + Vektor rechnen (staubsauger#14). Reine Bildlogik, kein AWS.

YuNet (Erkennung) + SFace (128-d-Vektor), OpenCV, beide Apache-2.0.
Vektoren sind L2-normiert -> Kosinus-Ähnlichkeit = Skalarprodukt.
"""
import io
import os
import threading

import cv2
import numpy as np
from PIL import Image, ImageFile, ImageOps

MODEL_DIR = os.environ.get("PHOTON_MODEL_DIR", os.path.join(os.path.dirname(__file__), "models"))
YUNET = "face_detection_yunet_2023mar.onnx"
SFACE = "face_recognition_sface_2021dec.onnx"

MAXDIM = int(os.environ.get("PHOTON_FACE_MAXDIM", "1600"))  # lange Kante für die Erkennung
MIN_SCORE = 0.8   # YuNet-Konfidenz
MIN_SIZE = 24     # px (auf MAXDIM-Bild); kleinere Gesichter taugen nicht zum Wiedererkennen
CROP_PX = 160     # Ausschnitt für die Benennen-Ansicht

Image.MAX_IMAGE_PIXELS = 300_000_000
ImageFile.LOAD_TRUNCATED_IMAGES = True

_tls = threading.local()  # OpenCV-Netze sind nicht thread-sicher -> je Thread eigene


def _m():
    if not hasattr(_tls, "m"):
        _tls.m = {}
    return _tls.m


def _det(w, h):
    d = _m().get("det")
    if d is None:
        d = _m()["det"] = cv2.FaceDetectorYN.create(
            os.path.join(MODEL_DIR, YUNET), "", (w, h), MIN_SCORE, 0.3, 5000)
    d.setInputSize((w, h))
    return d


def _rec():
    r = _m().get("rec")
    if r is None:
        r = _m()["rec"] = cv2.FaceRecognizerSF.create(os.path.join(MODEL_DIR, SFACE), "")
    return r


def load_image(data: bytes) -> np.ndarray:
    """Bytes -> BGR-Array, Drehung laut EXIF, lange Kante <= MAXDIM."""
    im = Image.open(io.BytesIO(data))
    if im.format == "JPEG":
        im.draft("RGB", (MAXDIM, MAXDIM))  # JPEG direkt verkleinert dekodieren (schnell, wenig RAM)
    im = ImageOps.exif_transpose(im).convert("RGB")
    im.thumbnail((MAXDIM, MAXDIM), Image.LANCZOS)
    return cv2.cvtColor(np.asarray(im), cv2.COLOR_RGB2BGR)


def find_faces(img: np.ndarray) -> list[dict]:
    """Alle Gesichter im Bild: Box (relativ 0..1), Score, Vektor (float32[128], normiert)."""
    h, w = img.shape[:2]
    _, raw = _det(w, h).detect(img)
    out = []
    for f in (raw if raw is not None else []):
        x, y, fw, fh, score = float(f[0]), float(f[1]), float(f[2]), float(f[3]), float(f[14])
        if score < MIN_SCORE or min(fw, fh) < MIN_SIZE:
            continue
        aligned = _rec().alignCrop(img, f)
        vec = _rec().feature(aligned).flatten().astype(np.float32)
        vec /= np.linalg.norm(vec) or 1.0
        out.append({
            "box": [round(max(x, 0) / w, 4), round(max(y, 0) / h, 4), round(fw / w, 4), round(fh / h, 4)],
            "score": round(score, 3),
            "px": int(min(fw, fh)),
            "vec": vec,
            "crop": crop_jpeg(img, x, y, fw, fh),
        })
    out.sort(key=lambda d: -d["px"])
    return out


def crop_jpeg(img, x, y, fw, fh, pad=0.35) -> bytes:
    """Quadratischer Ausschnitt um das Gesicht (mit Rand), CROP_PX groß, als JPEG."""
    h, w = img.shape[:2]
    side = max(fw, fh) * (1 + 2 * pad)
    cx, cy = x + fw / 2, y + fh / 2
    x0, y0 = int(max(cx - side / 2, 0)), int(max(cy - side / 2, 0))
    x1, y1 = int(min(cx + side / 2, w)), int(min(cy + side / 2, h))
    c = cv2.resize(img[y0:y1, x0:x1], (CROP_PX, CROP_PX), interpolation=cv2.INTER_AREA)
    ok, buf = cv2.imencode(".jpg", c, [cv2.IMWRITE_JPEG_QUALITY, 85])
    return buf.tobytes() if ok else b""


def similarity(a: np.ndarray, b: np.ndarray) -> float:
    return float(np.dot(a, b))


def vec_to_bytes(v: np.ndarray) -> bytes:
    return np.asarray(v, dtype=np.float16).tobytes()  # 256 Byte je Gesicht, Genauigkeit reicht


def bytes_to_vec(b: bytes) -> np.ndarray:
    return np.frombuffer(bytes(b), dtype=np.float16).astype(np.float32)
