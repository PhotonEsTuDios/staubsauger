"""PhotonEsTuDios GPS-Extraktion — liest EXIF-GPS aus den S3-Originalen und
schreibt gps_lat/gps_lon ins DynamoDB-Item. Unabhaengig vom quality+color-Labeler
(eigene Marke `gps_version`), laeuft also auch ueber schon gelabelte Fotos.

Schnell: zieht nur die ersten 256 KB per Range-Request (EXIF-APP1 sitzt am
Dateianfang) statt der ganzen (teils GB-grossen) Datei. Nur Pillow, kein neuer Dep.
Idempotent/resumierbar via attribute_not_exists(gps_version).

  AWS_PROFILE=photon python gps_extract.py [--limit N] [--workers N]
"""
import io
import os
import sys
import time
import threading
from decimal import Decimal
from concurrent.futures import ThreadPoolExecutor

import boto3
from PIL import Image, ExifTags

REGION = "eu-west-1"
TABLE = os.environ.get("PHOTON_TABLE", "photonestudios-photos")
BUCKET = os.environ.get("PHOTON_BUCKET", "photonestudios-photos")
GPS_VERSION = 1
WORKERS = int(os.environ.get("PHOTON_GPS_WORKERS", "12"))
HEADER_BYTES = 262144  # erste 256 KB -> EXIF-Header
IMG_EXTS = {".jpg", ".jpeg", ".png", ".heic", ".heif", ".tif", ".tiff", ".webp"}

ddb = boto3.resource("dynamodb", region_name=REGION).Table(TABLE)
s3 = boto3.client("s3", region_name=REGION)

_lock = threading.Lock()
counts = {"checked": 0, "with_gps": 0, "errors": 0}
_t0 = time.time()


def _dms(v):
    d, m, s = [float(x) for x in v]
    return d + m / 60.0 + s / 3600.0


def extract_gps(img):
    exif = img.getexif()
    if not exif:
        return None
    gps = exif.get_ifd(ExifTags.IFD.GPSInfo)
    if not gps:
        return None
    lat, lat_ref = gps.get(2), gps.get(1)
    lon, lon_ref = gps.get(4), gps.get(3)
    if not lat or not lon:
        return None
    la, lo = _dms(lat), _dms(lon)
    if str(lat_ref).upper().startswith("S"):
        la = -la
    if str(lon_ref).upper().startswith("W"):
        lo = -lo
    if not (-90 <= la <= 90 and -180 <= lo <= 180) or (la == 0 and lo == 0):
        return None
    return round(la, 6), round(lo, 6)


def process(item):
    sha, key = item["sha256"], item["s3_key"]
    coords = None
    try:
        obj = s3.get_object(Bucket=BUCKET, Key=key,
                            Range=f"bytes=0-{HEADER_BYTES - 1}")
        data = obj["Body"].read()
        try:
            coords = extract_gps(Image.open(io.BytesIO(data)))
        except Exception:
            coords = None  # Header abgeschnitten/Format -> kein GPS
        expr = "SET gps_version=:v, gps_checked_at=:t, has_gps=:h"
        vals = {
            ":v": GPS_VERSION,
            ":t": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
            ":h": coords is not None,
        }
        if coords:
            expr += ", gps_lat=:la, gps_lon=:lo"
            vals[":la"] = Decimal(str(coords[0]))
            vals[":lo"] = Decimal(str(coords[1]))
        ddb.update_item(Key={"sha256": sha}, UpdateExpression=expr,
                        ExpressionAttributeValues=vals)
        return "with_gps" if coords else "checked"
    except Exception as e:
        if os.environ.get("PHOTON_GPS_DEBUG"):
            print(f"  err {key}: {e!r}", file=sys.stderr)
        return "errors"


def tally(r):
    with _lock:
        counts["checked"] += 1
        if r == "with_gps":
            counts["with_gps"] += 1
        elif r == "errors":
            counts["errors"] += 1
        c = counts["checked"]
        if c % 50 == 0:
            rate = c / max(1e-9, time.time() - _t0) * 3600
            print(f"  {c} geprueft, {counts['with_gps']} mit GPS  ({rate:,.0f}/h)", flush=True)


def main():
    limit = None
    if "--limit" in sys.argv:
        limit = int(sys.argv[sys.argv.index("--limit") + 1])
    print(f"GPS-Extraktion startet — {WORKERS} Worker, limit={limit}", flush=True)

    # Kandidaten (Bilder, noch nicht geprueft) bis zum Limit einsammeln
    candidates, kw = [], {
        "ProjectionExpression": "sha256, s3_key",
        "FilterExpression": "attribute_exists(s3_key) AND attribute_not_exists(gps_version)",
    }
    while True:
        resp = ddb.scan(**kw)
        for it in resp.get("Items", []):
            if str(it["sha256"]).startswith("__"):
                continue
            if os.path.splitext(it.get("s3_key", ""))[1].lower() not in IMG_EXTS:
                continue
            candidates.append(it)
        if limit and len(candidates) >= limit:
            candidates = candidates[:limit]
            break
        if "LastEvaluatedKey" not in resp:
            break
        kw["ExclusiveStartKey"] = resp["LastEvaluatedKey"]

    print(f"  {len(candidates)} Kandidaten", flush=True)
    with ThreadPoolExecutor(max_workers=WORKERS) as pool:
        for r in pool.map(process, candidates):
            tally(r)

    dt = time.time() - _t0
    print(f"\nFertig nach {dt:.0f}s — geprueft={counts['checked']} "
          f"mit_GPS={counts['with_gps']} err={counts['errors']}", flush=True)


if __name__ == "__main__":
    main()
