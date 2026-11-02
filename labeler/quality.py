"""Qualitaets- + Farb-Analyse eines Bildes (Label-Version 1).

Gemeinsam genutzt vom lokalen labeler.py und der Cloud-Funktion labeler_cloud/
(staubsauger#6), damit beide exakt dieselben Werte schreiben. Nur Pillow + numpy.
"""
import numpy as np
from PIL import Image

LABEL_VERSION = 1
MAXDIM = 1024  # auf diese Kantenlaenge runterskalieren -> konsistente Schaerfe-Skala


def analyze(img: Image.Image) -> dict:
    """Qualitaets- + Farb-Metriken aus einem PIL-Image."""
    im = img.convert("RGB")
    im.thumbnail((MAXDIM, MAXDIM))

    # --- Qualitaet ---
    g = np.asarray(im.convert("L"), dtype=np.float64)
    # diskreter Laplace ueber den Innenbereich -> Varianz = Schaerfe
    lap = (4 * g[1:-1, 1:-1] - g[:-2, 1:-1] - g[2:, 1:-1]
           - g[1:-1, :-2] - g[1:-1, 2:])
    sharpness = int(round(float(lap.var())))
    brightness = int(round(float(g.mean())))
    contrast = int(round(float(g.std())))

    sharp_score = min(100.0, sharpness / 5.0)           # ~500 Varianz -> 100
    bright_ok = max(0.0, 100.0 - abs(brightness - 128) * 0.6)
    quality_score = int(max(0, min(100, round(0.7 * sharp_score + 0.3 * bright_ok))))
    blurry = sharpness < 100

    # --- Farbe: Dominanzfarben via adaptive Quantisierung ---
    q = im.quantize(colors=6, method=Image.Quantize.MEDIANCUT)
    pal = q.getpalette()
    colcounts = sorted(q.getcolors(), reverse=True)      # (count, index)
    dominant = []
    for _cnt, idx in colcounts[:5]:
        r, gg, b = pal[idx * 3:idx * 3 + 3]
        dominant.append("#%02x%02x%02x" % (r, gg, b))

    return {
        "sharpness": sharpness,
        "brightness": brightness,
        "contrast": contrast,
        "quality_score": quality_score,
        "blurry": blurry,
        "dominant_colors": dominant,
    }
